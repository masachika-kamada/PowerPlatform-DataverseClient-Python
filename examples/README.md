# PowerPlatform Dataverse Client Examples

This directory contains comprehensive examples demonstrating how to use the **PowerPlatform-Dataverse-Client** SDK for Python.

## 📦 Installation

Install the PowerPlatform Dataverse Client SDK:

```bash
pip install PowerPlatform-Dataverse-Client
```

## 📁 Directory Structure

### 🌱 Basic Examples (`basic/`)
Get started quickly with fundamental Dataverse operations:
- **`quickstart.py`** - Basic client setup, authentication, and simple CRUD operations
- Authentication setup with Azure Identity
- Creating, reading, updating, and deleting records
- Basic error handling

### 🚀 Advanced Examples (`advanced/`)
Explore powerful features for complex scenarios:
- **`file_upload.py`** - File upload to Dataverse file columns with chunking
- **`pandas_integration.py`** - DataFrame-based operations for data analysis

## 🚀 Getting Started

1. **Install the SDK**:
   ```bash
   pip install PowerPlatform-Dataverse-Client
   ```

2. **Install Additional Dependencies** (for examples):
   ```bash
   pip install azure-identity pandas
   ```

2. **Set Up Authentication**:
   Configure Azure Identity credentials (see individual examples for details)

3. **Run Basic Example**:
   ```bash
   python examples/basic/quickstart.py
   ```

## 📋 Prerequisites

- Python 3.10+
- PowerPlatform-Dataverse-Client SDK installed (`pip install PowerPlatform-Dataverse-Client`)
- Azure Identity credentials configured
- Access to a Dataverse environment

## 🔒 Authentication

All examples use Azure Identity for authentication. Common patterns:
- `DefaultAzureCredential` for development
- `ClientSecretCredential` for production services
- `InteractiveBrowserCredential` for interactive scenarios

## 📖 Documentation

For detailed API documentation, visit: [Dataverse SDK Documentation](link-to-docs)

## 🤝 Contributing

When adding new examples:
1. Follow the existing code style and structure
2. Include comprehensive comments and docstrings
3. Add error handling and validation
4. Update this README with your example
5. Test thoroughly before submitting