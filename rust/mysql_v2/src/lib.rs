//! C ABI for the real mysql-client-lib client. Transport callbacks yield to
//! sysbench's coroutine on the same worker; this crate creates no OS threads.
use std::alloc::{GlobalAlloc, Layout};
use std::ffi::c_void;
use std::fmt::Write as _;
use std::io::{self, Read, Write};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::slice;

use mysql_client::protocol::CLIENT_CONNECT_ATTRS;
use mysql_client::{
    Client, ConnectOptions, Error, PreparedStatement, QueryResult, Transport, Value,
};

unsafe extern "C" {
    fn sb_mysql_v2_allocate(size: usize, alignment: usize) -> *mut u8;
    fn sb_mysql_v2_deallocate(pointer: *mut u8);
}

struct ArenaAllocator;
unsafe impl GlobalAlloc for ArenaAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { sb_mysql_v2_allocate(layout.size(), layout.align()) }
    }
    unsafe fn dealloc(&self, pointer: *mut u8, _: Layout) {
        unsafe { sb_mysql_v2_deallocate(pointer) }
    }
}
#[global_allocator]
static ALLOCATOR: ArenaAllocator = ArenaAllocator;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Bytes {
    data: *const u8,
    size: usize,
}
impl Bytes {
    fn borrowed(bytes: &[u8]) -> Self {
        Self {
            data: bytes.as_ptr(),
            size: bytes.len(),
        }
    }
    unsafe fn slice<'a>(self) -> &'a [u8] {
        if self.size == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(self.data, self.size) }
        }
    }
    unsafe fn text<'a>(self) -> mysql_client::Result<&'a str> {
        std::str::from_utf8(unsafe { self.slice() })
            .map_err(|_| Error::Protocol("SQL and connection options must be UTF-8"))
    }
}

#[repr(C)]
pub struct FfiError {
    code: i32,
    fatal: u8,
    state: [u8; 6],
    message: [u8; 1024],
}
impl FfiError {
    fn clear(&mut self) {
        self.code = 0;
        self.fatal = 0;
        self.state = *b"00000\0";
        self.message.fill(0);
    }
    fn set(&mut self, error: &Error) {
        self.clear();
        self.code = 2027;
        self.fatal = 1;
        self.state = *b"HY000\0";
        if let Error::Server(server) = error {
            self.code = i32::from(server.code);
            self.fatal = u8::from(!matches!(server.code, 1205 | 1213));
            self.state[..5].copy_from_slice(server.state.as_str().as_bytes());
        } else if matches!(error, Error::Io(_)) {
            self.code = 2013;
        } else if matches!(error, Error::Tls(_)) {
            self.code = 2026;
        }
        struct Buffer<'a> {
            bytes: &'a mut [u8],
            used: usize,
        }
        impl std::fmt::Write for Buffer<'_> {
            fn write_str(&mut self, text: &str) -> std::fmt::Result {
                let take = text.len().min(self.bytes.len() - self.used);
                self.bytes[self.used..self.used + take].copy_from_slice(&text.as_bytes()[..take]);
                self.used += take;
                Ok(())
            }
        }
        let mut buffer = Buffer {
            bytes: &mut self.message[..1023],
            used: 0,
        };
        if let Error::Server(server) = error {
            let _ = write!(buffer, "{}", server.message);
        } else {
            let _ = write!(buffer, "{error}");
        }
    }
}

#[repr(C)]
pub struct Parameter {
    kind: u32,
    integer: i64,
    real: f64,
    bytes: Bytes,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Options {
    user: Bytes,
    password: Bytes,
    database: Bytes,
    max_packet_size: u32,
    tls_mode: u32,
    tls_verify_server_cert: u8,
    server_name: Bytes,
    tls_ca: Bytes,
    tls_cert: Bytes,
    tls_key: Bytes,
    context: *mut c_void,
    read: unsafe extern "C" fn(*mut c_void, *mut u8, usize) -> isize,
    write: unsafe extern "C" fn(*mut c_void, *const u8, usize) -> isize,
}

struct CallbackTransport(Options);
impl Read for CallbackTransport {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let count = unsafe { (self.0.read)(self.0.context, bytes.as_mut_ptr(), bytes.len()) };
        if count < 0 {
            Err(io::Error::from_raw_os_error(5))
        } else {
            Ok(count as usize)
        }
    }
}
impl Write for CallbackTransport {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let count = unsafe { (self.0.write)(self.0.context, bytes.as_ptr(), bytes.len()) };
        if count < 0 {
            Err(io::Error::from_raw_os_error(5))
        } else {
            Ok(count as usize)
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Transport for CallbackTransport {}
type Session = Client<CallbackTransport>;

struct ResultHandle {
    result: QueryResult,
    scratch: String,
}

#[repr(C)]
pub struct Command {
    operation: u32,
    client: *mut c_void,
    statement: *mut c_void,
    result: *mut c_void,
    sql: Bytes,
    parameters: *const Parameter,
    parameter_count: usize,
    options: Options,
    error: FfiError,
}

fn drain(client: &mut Session, mut first: QueryResult) -> mysql_client::Result<QueryResult> {
    // An EOF-style RETURNING result can carry its OK as the next result. Keep
    // those counts with the returned rows, then drain the entire command.
    while let Some(next) = client.next_result()? {
        if first.ok.is_none() && next.columns.is_empty() {
            first.ok = next.ok;
        }
    }
    first.has_more_results = false;
    Ok(first)
}

unsafe fn dispatch(command: &mut Command) -> mysql_client::Result<()> {
    match command.operation {
        0 => {
            let mut options =
                ConnectOptions::new(unsafe { command.options.user.text()? }, unsafe {
                    command.options.password.text()?
                });
            if command.options.database.size != 0 {
                options.database = Some(unsafe { command.options.database.text()? }.to_owned());
            }
            options.max_packet_size = command.options.max_packet_size;
            if command.options.tls_mode != 0 {
                let mut tls =
                    mysql_client::TlsOptions::new(unsafe { command.options.server_name.text()? });
                tls.mode = match command.options.tls_mode {
                    1 => mysql_client::TlsMode::Preferred,
                    2 => mysql_client::TlsMode::Required,
                    _ => return Err(Error::Protocol("invalid TLS mode")),
                };
                tls.verify_server_cert = command.options.tls_verify_server_cert != 0;
                if command.options.tls_ca.size != 0 {
                    tls.ca_file = Some(unsafe { command.options.tls_ca.text()? }.into());
                }
                if command.options.tls_cert.size != 0 {
                    tls.client_cert_file = Some(unsafe { command.options.tls_cert.text()? }.into());
                }
                if command.options.tls_key.size != 0 {
                    tls.client_key_file = Some(unsafe { command.options.tls_key.text()? }.into());
                }
                options.tls = Some(tls);
            }
            options.connect_attrs = vec![
                ("_client_name".into(), "sysbench-cpp".into()),
                ("sqlrust_protocol".into(), "2".into()),
            ];
            let client = Client::connect_over(CallbackTransport(command.options), options)?;
            if client.handshake().capability_flags & CLIENT_CONNECT_ATTRS == 0 {
                return Err(Error::Protocol(
                    "server lacks connection attributes required for sqlrust_protocol=2",
                ));
            }
            command.client = Box::into_raw(Box::new(client)).cast();
        }
        1 => {
            let client = unsafe { &mut *command.client.cast::<Session>() };
            let first = client.query(unsafe { command.sql.text()? })?;
            let result = drain(client, first)?;
            command.result = Box::into_raw(Box::new(ResultHandle {
                result,
                scratch: String::new(),
            }))
            .cast();
        }
        2 => {
            let client = unsafe { &mut *command.client.cast::<Session>() };
            let statement = client.prepare(unsafe { command.sql.text()? })?;
            command.statement = Box::into_raw(Box::new(statement)).cast();
        }
        3 => {
            let client = unsafe { &mut *command.client.cast::<Session>() };
            let statement = unsafe { &*command.statement.cast::<PreparedStatement>() };
            let parameters = if command.parameter_count == 0 {
                &[]
            } else {
                unsafe { slice::from_raw_parts(command.parameters, command.parameter_count) }
            };
            if parameters.len() != statement.params.len() {
                return Err(Error::Protocol(
                    "wrong number of prepared statement parameters",
                ));
            }
            let values = parameters
                .iter()
                .map(|parameter| match parameter.kind {
                    0 => Value::Null,
                    1 => Value::Int(parameter.integer),
                    2 => Value::Double(parameter.real),
                    _ => Value::Bytes(unsafe { parameter.bytes.slice() }.to_vec()),
                })
                .collect::<Vec<_>>();
            let first = client.execute(statement, &values)?;
            let result = drain(client, first)?;
            command.result = Box::into_raw(Box::new(ResultHandle {
                result,
                scratch: String::new(),
            }))
            .cast();
        }
        4 => {
            let client = unsafe { &mut *command.client.cast::<Session>() };
            let statement = unsafe { &*command.statement.cast::<PreparedStatement>() };
            client.stmt_close(statement.statement_id)?;
        }
        _ => return Err(Error::Protocol("unknown MySQL v2 bridge operation")),
    }
    Ok(())
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sb_mysql_v2_run(command: *mut Command) {
    let command = unsafe { &mut *command };
    command.error.clear();
    match catch_unwind(AssertUnwindSafe(|| unsafe { dispatch(command) })) {
        Ok(Ok(())) => {}
        Ok(Err(error)) => command.error.set(&error),
        Err(_) => command
            .error
            .set(&Error::Protocol("Rust MySQL client panicked")),
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sb_mysql_v2_drop_client(pointer: *mut c_void) {
    if !pointer.is_null() {
        drop(unsafe { Box::from_raw(pointer.cast::<Session>()) });
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sb_mysql_v2_drop_statement(pointer: *mut c_void) {
    if !pointer.is_null() {
        drop(unsafe { Box::from_raw(pointer.cast::<PreparedStatement>()) });
    }
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sb_mysql_v2_drop_result(pointer: *mut c_void) {
    if !pointer.is_null() {
        drop(unsafe { Box::from_raw(pointer.cast::<ResultHandle>()) });
    }
}

#[repr(C)]
pub struct ResultInfo {
    columns: usize,
    rows: usize,
    affected_rows: u64,
    last_insert_id: u64,
    warnings: u16,
    status: u16,
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sb_mysql_v2_result_info(pointer: *const c_void) -> ResultInfo {
    let result = &unsafe { &*pointer.cast::<ResultHandle>() }.result;
    let mut info = ResultInfo {
        columns: result.columns.len(),
        rows: result.rows.len(),
        affected_rows: 0,
        last_insert_id: 0,
        warnings: 0,
        status: 0,
    };
    if let Some(ok) = &result.ok {
        info.affected_rows = ok.affected_rows;
        info.last_insert_id = ok.last_insert_id;
        info.warnings = ok.warnings;
        info.status = ok.status_flags;
    } else if let Some(eof) = &result.eof {
        info.warnings = eof.warnings;
        info.status = eof.status_flags;
    }
    info
}

#[repr(C)]
pub struct Column {
    name: Bytes,
    column_type: u8,
    flags: u16,
    charset: u16,
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sb_mysql_v2_column(pointer: *const c_void, index: usize) -> Column {
    let column = &unsafe { &*pointer.cast::<ResultHandle>() }.result.columns[index];
    Column {
        name: Bytes::borrowed(&column.name),
        column_type: column.column_type,
        flags: column.flags,
        charset: column.character_set,
    }
}

#[repr(C)]
pub struct Field {
    bytes: Bytes,
    is_null: u8,
}
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sb_mysql_v2_field(
    pointer: *mut c_void,
    row: usize,
    column: usize,
) -> Field {
    let handle = unsafe { &mut *pointer.cast::<ResultHandle>() };
    let value = &handle.result.rows[row][column];
    if let Value::Bytes(bytes) = value {
        return Field {
            bytes: Bytes::borrowed(bytes),
            is_null: 0,
        };
    }
    handle.scratch.clear();
    match value {
        Value::Null => {
            return Field {
                bytes: Bytes::borrowed(&[]),
                is_null: 1,
            };
        }
        Value::Int(value) => {
            let _ = write!(handle.scratch, "{value}");
        }
        Value::UInt(value) => {
            let _ = write!(handle.scratch, "{value}");
        }
        Value::Float(value) => {
            let _ = write!(handle.scratch, "{value}");
        }
        Value::Double(value) => {
            let _ = write!(handle.scratch, "{value}");
        }
        Value::Date {
            year,
            month,
            day,
            hour,
            minute,
            second,
            micros,
        } => {
            let _ = write!(handle.scratch, "{year:04}-{month:02}-{day:02}");
            if handle.result.columns[column].column_type != mysql_client::protocol::MYSQL_TYPE_DATE
            {
                let _ = write!(handle.scratch, " {hour:02}:{minute:02}:{second:02}");
                if *micros != 0 {
                    let _ = write!(handle.scratch, ".{micros:06}");
                }
            }
        }
        Value::Time {
            negative,
            days,
            hours,
            minutes,
            seconds,
            micros,
        } => {
            let sign = if *negative { "-" } else { "" };
            let hours = u64::from(*days) * 24 + u64::from(*hours);
            let _ = write!(handle.scratch, "{sign}{hours:02}:{minutes:02}:{seconds:02}");
            if *micros != 0 {
                let _ = write!(handle.scratch, ".{micros:06}");
            }
        }
        Value::Bytes(_) => unreachable!(),
    }
    Field {
        bytes: Bytes::borrowed(handle.scratch.as_bytes()),
        is_null: 0,
    }
}
