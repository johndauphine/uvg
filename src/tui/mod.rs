mod actions;
mod app;
mod events;
mod render;

use anyhow::Result;

use crate::cli::Cli;

pub async fn run(cli: Cli) -> Result<()> {
    let mut terminal = ratatui::init();
    let mut app = app::App::new(&cli);
    let result = events::event_loop(&mut terminal, &mut app).await;
    ratatui::restore();
    result
}

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
