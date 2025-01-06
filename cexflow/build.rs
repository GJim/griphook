use shadow_rs::{SdResult, ShadowBuilder};

fn main() -> SdResult<()> {
    let _unused = ShadowBuilder::builder().build()?;
    Ok(())
}
