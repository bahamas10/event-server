use anyhow::Result;

use event_server::Event;

fn main() -> Result<()> {
    let body: Vec<Event> =
        reqwest::blocking::get("http://localhost:3000/events")?.json()?;

    println!("{:#?}", body);

    Ok(())
}
