<p align="center">
  <!-- project logo --!>
  <img src="blip.png" alt="logo"><br><br>
  <!-- docs.rs --!>
  <a href="https://docs.rs/blip">
    <img alt="Docs.rs" src="https://docs.rs/blip/badge.svg">
  </a>
  <!-- crates.io version !-->
  <a href="https://crates.io/crates/blip">
    <img alt="Crates.io" src="https://img.shields.io/crates/v/blip?style=flat-square">
  </a>
  <!-- crates.io downloads --!>
  <a href="https://crates.io/crates/blip">
    <img alt="Crates.io" src="https://img.shields.io/crates/d/blip?style=flat-square">
  </a>
  <!-- crates.io license --!>
  <a href="./LICENSE-APACHE">
    <img alt="Apache-2.0 OR MIT" src="https://img.shields.io/crates/l/blip?style=flat-square">
  </a>
</p>

A crate for writing fast and highly resilient in-process gRPC service meshes.

# Status
Extremely alpha, you ~~probably~~ definitely shouldn't use this. It's just an experiment for now.

# Overview
`blip` provides an implementation of distributed membership based on [rapid][rapid], exposed
as a gRPC service. Groups of servers become aware of each other through the membership
protocol, and any given member may expose its own metadata or linked services through
the same backing gRPC server.

In essence, this crate provides a membership list with strong consistency semantics
(as opposed to weakly consistent protocols like [SWIM][SWIM]), distributed fault detection,
and grpc routing.

# Service Discovery
`blip` is designed to build heterogenous meshes. As such, members may expose arbitrary
(immutable) key-value metadata when they join a mesh, which can be used for the purpose
of service discovery.

# Sharding and State
`blip` does not enforce any invariants with regard to state held by members of a mesh.
For maximal flexibility, state and sharding are deferred to implementations of member
services.

# Feature Flags
* `cache`: Enables the cache service.

# References
* [Stable and Consistent Membership at Scale with Rapid][rapid]
* [Fast Paxos][fpx]

[rapid]: https://arxiv.org/abs/1803.03620
[fpx]: https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-2005-112.pdf
[SWIM]: https://www.cs.cornell.edu/projects/Quicksilver/public_pdfs/SWIM.pdf

# License
Licensed under either of

 * Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
 * MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Contribution
Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any additional terms or conditions.
