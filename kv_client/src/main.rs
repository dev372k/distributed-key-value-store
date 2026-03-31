use std::env;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 3 {
        println!("Usage:");
        println!("  put <url> <key> <value>");
        println!("  get <url> <key>");
        println!("  delete <url> <key>");
        return;
    }

    let command = &args[1];
    let base_url = &args[2];

    let url = match command.as_str() {
        "put" => {
            if args.len() != 5 {
                panic!("Usage: put <url> <key> <value>");
            }
            format!("{}/put?key={}&value={}", base_url, args[3], args[4])
        }
        "get" => {
            if args.len() != 4 {
                panic!("Usage: get <url> <key>");
            }
            format!("{}/get?key={}", base_url, args[3])
        }
        "delete" => {
            if args.len() != 4 {
                panic!("Usage: delete <url> <key>");
            }
            format!("{}/delete?key={}", base_url, args[3])
        }
        _ => {
            panic!("Unknown command");
        }
    };

    let response = reqwest::blocking::get(&url)
        .expect("Request failed")
        .text()
        .expect("Failed to read response");

    println!("{}", response);
}