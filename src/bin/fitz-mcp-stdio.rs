use std::process::ExitCode;

fn main() -> ExitCode {
    match fitz::api::mcp::run_stdio_adapter() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("fitz-mcp-stdio: {error}");
            ExitCode::FAILURE
        }
    }
}
