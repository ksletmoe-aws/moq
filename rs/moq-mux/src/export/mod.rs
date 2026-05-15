//! Subscribe to a moq broadcast and decode media frames.
//!
//! [`Fmp4`] subscribes to a broadcast, decodes every track via
//! [`Consumer<Hang>`](crate::container::Consumer), and yields a single fMP4 / CMAF byte
//! stream. The merged init segment is followed by moof+mdat fragments in
//! timestamp order across tracks.
//!
//! The [`cmsf`] module provides CMSF-native demuxing without the hang container layer.

pub mod cmsf;
mod fmp4;

pub use fmp4::*;
