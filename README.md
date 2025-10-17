# Webtapu Extractor App

WebtapuApp is a Flask-based web application for processing Turkish property documents (tapu belgeleri) and extracting structured data from PDF files. The application provides a user-friendly interface for uploading PDFs, processing them, and downloading the extracted data in Excel or CSV format.

## Features

- **PDF Processing**: Extract data from Turkish property documents using Camelot
- **Watermark Removal**: Automatic removal of watermarks from PDF files
- **Turkish Text Processing**: Proper handling of Turkish characters and text normalization using ICU
- **Multiple Output Formats**: Generate Excel (.xlsx) and CSV (.csv) files
- **Batch Processing**: Process multiple PDF files simultaneously
- **Web Interface**: Simple and intuitive web interface built with Bootstrap 5

## Technology Stack

- **Backend**: Flask, Python 3.11+
- **PDF Processing**: Camelot, PyMuPDF (fitz)
- **Text Processing**: PyICU for Turkish text normalization
- **Data Processing**: Pandas
- **Frontend**: HTML5, Bootstrap 5, JavaScript
- **Package Management**: UV

## Installation

### Prerequisites

- Python 3.11 or higher
- UV package manager

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd WebtapuApp
```

2. Install dependencies using UV:
```bash
uv sync
```

3. Run the application:
```bash
uv run python app.py
```

4. Open your browser and navigate to `http://localhost:5000`

## Usage

1. **Upload PDFs**: Click "Dosya Seç" to select one or multiple PDF files
2. **Choose Output Format**: Select Excel or CSV format
3. **Processing Options**: Enable/disable watermark removal
4. **Process**: Click "PDF'leri İşle" to start processing
5. **Download**: The processed file will be automatically downloaded

## Supported Data Extraction

The application extracts the following information from Turkish property documents:

- Taşınmaz kimlik numarası
- İl, ilçe, mahalle bilgileri
- Ada, parsel, blok bilgileri
- Kat ve daire numaraları
- Haciz türleri ve açıklamaları
- İcra dairesi bilgileri
- Dosya numaraları ve tarihler
- Yevmiye numaraları

## Project Structure

```
WebtapuApp/
├── app.py                 # Main Flask application
├── pdf_processor.py       # Main PDF processing coordinator
├── text_processor.py      # Turkish text processing utilities
├── watermark_remover.py   # Watermark removal functionality
├── table_extractor.py     # Table extraction using Camelot
├── data_extractor.py      # Data extraction and processing
├── pyproject.toml         # Project dependencies and configuration
├── templates/             # HTML templates
│   ├── base.html
│   ├── index.html
│   └── about.html
└── README.md
```

## Development

### Running in Development Mode

```bash
uv run python app.py
```

### Installing Development Dependencies

```bash
uv sync --group dev
```

### Running Tests

```bash
uv run pytest
```

## Configuration

The application can be configured through environment variables:

- `SECRET_KEY`: Flask secret key for session security
- `UPLOAD_FOLDER`: Directory for temporary file uploads (default: system temp)
- `MAX_CONTENT_LENGTH`: Maximum file upload size (default: 100MB)

## Dependencies

### Core Dependencies

- `flask>=3.0.0`: Web framework
- `pandas>=2.3.3`: Data processing
- `camelot-py>=1.0.9`: PDF table extraction
- `pymupdf>=1.26.4`: PDF processing and watermark removal
- `pyicu>=2.15.3`: Turkish text normalization
- `tqdm>=4.67.1`: Progress bars

### Development Dependencies

- `pytest>=7.0.0`: Testing framework
- `black>=23.0.0`: Code formatting
- `flake8>=6.0.0`: Code linting
- `mypy>=1.0.0`: Type checking

## License

This project is licensed under the MIT License.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## Support

For issues and questions, please open an issue on the project repository.
