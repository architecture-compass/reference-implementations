# ArchitectureCompass reference implementations

Runnable code for the reference architectures on [**ArchitectureCompass**](https://architecturecompass.com), a vendor-neutral field guide to software architecture.

Each architecture is implemented in several languages against **one shared contract**, so you can compare how the same patterns look in each ecosystem. Everything runs locally with Docker Compose and needs no cloud account or API keys.

| Architecture | Java | Python | Go | Rust | C# | TypeScript | Read about it |
|---|:-:|:-:|:-:|:-:|:-:|:-:|---|
| [Microservices](microservices/) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | [Microservices](https://architecturecompass.com/architectures/software-styles/microservices/) |
| [RAG](rag/) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | [Retrieval-augmented generation](https://architecturecompass.com/architectures/ai-ml/rag/) |
| [Lakehouse](lakehouse/) | | | | | | | [Lakehouse](https://architecturecompass.com/architectures/data/lakehouse/) |

## Quick start

```bash
git clone https://github.com/architecture-compass/reference-implementations
cd reference-implementations/microservices
docker compose --profile go up -d --build      # or java, python, rust, dotnet, node
./demo.sh
```

## How each architecture folder is organized

- `README.md`: what the architecture demonstrates, how to run it, and where to look in each language
- `CONTRACT.md`: the API, events and behavior every language must implement
- `docker-compose.yml`: shared infrastructure, plus one Compose profile per language
- `demo.sh`: an automated check that proves the key behaviors; every language passes it
- one sub-folder per language

## Requirements

- Docker with Compose v2
- About 4 GB of free memory for the larger stacks

## License

[MIT](LICENSE). Use these freely in your own work.

Written by [Abhinav Taduka](https://architecturecompass.com/author/), founder and editor of ArchitectureCompass.
