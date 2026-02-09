"""
Enhanced Universal Data File Loader
Version: 2.0.0
Author: Data Engineering Team
Description: Robust multi-format data loader with intelligent auto-detection,
             error recovery, and Streamlit integration.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Union, Dict, Any, Optional, Tuple, List, BinaryIO, Generator
import chardet
import mimetypes
import warnings
import io
import json
import csv
from datetime import datetime
import logging
import tempfile
import os
import functools
import time
import sys
from enum import Enum
from dataclasses import dataclass, field
from contextlib import contextmanager
import hashlib

# Optional imports with fallbacks
try:
    from memory_profiler import memory_usage
    MEMORY_PROFILER_AVAILABLE = True
except ImportError:
    MEMORY_PROFILER_AVAILABLE = False

try:
    import streamlit as st
    STREAMLIT_AVAILABLE = True
except ImportError:
    STREAMLIT_AVAILABLE = False

# ============================================================================
# FALLBACKS FOR OPTIONAL DEPENDENCIES
# ============================================================================

class DummyStreamlit:
    """Fallback class for when Streamlit is not installed."""
    
    @staticmethod
    def cache_data(show_spinner=None, persist=False, **kwargs):
        """Dummy cache_data decorator."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper
        return decorator
    
    def file_uploader(self, *args, **kwargs): return None
    def expander(self, *args, **kwargs):
        class DummyExpander:
            def __enter__(self): return self
            def __exit__(self, *args): pass
        return DummyExpander()
    def json(self, *args, **kwargs): pass
    def dataframe(self, *args, **kwargs): pass
    def caption(self, *args, **kwargs): pass
    def selectbox(self, *args, **kwargs): return None
    def error(self, *args, **kwargs): pass
    def warning(self, *args, **kwargs): pass
    def info(self, *args, **kwargs): pass
    def success(self, *args, **kwargs): pass
    def write(self, *args, **kwargs): pass
    def markdown(self, *args, **kwargs): pass

if not STREAMLIT_AVAILABLE:
    st = DummyStreamlit()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configure warnings
warnings.filterwarnings('ignore', category=UserWarning)

# ============================================================================
# ENUMS AND DATA CLASSES
# ============================================================================

class FileFormat(Enum):
    """Supported file formats."""
    CSV = "csv"
    EXCEL = "excel"
    EXCEL_OLD = "excel_old"
    PARQUET = "parquet"
    JSON = "json"
    UNKNOWN = "unknown"
    AUTO = "auto"

class CompressionFormat(Enum):
    """Supported compression formats."""
    GZIP = "gzip"
    BZIP2 = "bzip2"
    ZIP = "zip"
    XZ = "xz"
    ZSTD = "zstd"
    NONE = "none"

@dataclass
class FileMetadata:
    """Metadata about the loaded file."""
    file_path: str = "unknown"
    original_filename: str = "data"
    format_detected: FileFormat = FileFormat.UNKNOWN
    format_used: FileFormat = FileFormat.UNKNOWN
    confidence: float = 0.0
    size_bytes: int = 0
    size_mb: float = 0.0
    encoding: Optional[str] = None
    delimiter: Optional[str] = None
    sheets_loaded: List[str] = field(default_factory=list)
    compression: CompressionFormat = CompressionFormat.NONE
    shape: Optional[Union[Tuple[int, int], Dict[str, Tuple[int, int]]]] = None
    columns: List[str] = field(default_factory=list)
    dtypes: Dict[str, str] = field(default_factory=dict)
    load_time_seconds: float = 0.0
    memory_usage_mb: float = 0.0
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    hash_md5: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            'file_path': self.file_path,
            'original_filename': self.original_filename,
            'format_detected': self.format_detected.value,
            'format_used': self.format_used.value,
            'confidence': self.confidence,
            'size_bytes': self.size_bytes,
            'size_mb': self.size_mb,
            'encoding': self.encoding,
            'delimiter': self.delimiter,
            'sheets_loaded': self.sheets_loaded,
            'compression': self.compression.value,
            'shape': self.shape,
            'columns': self.columns,
            'dtypes': self.dtypes,
            'load_time_seconds': self.load_time_seconds,
            'memory_usage_mb': self.memory_usage_mb,
            'warnings': self.warnings,
            'errors': self.errors,
            'hash_md5': self.hash_md5,
            'timestamp': self.timestamp
        }

@dataclass
class ValidationResult:
    """Results of DataFrame validation."""
    is_valid: bool = True
    warnings: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    critical_errors: List[str] = field(default_factory=list)
    stats: Dict[str, Any] = field(default_factory=dict)

# ============================================================================
# CUSTOM EXCEPTIONS
# ============================================================================

class FileLoaderError(Exception):
    """Base exception for file loading errors."""
    pass

class FileTooLargeError(FileLoaderError):
    """Raised when file exceeds size limit."""
    pass

class InvalidFormatError(FileLoaderError):
    """Raised when file format is invalid or unsupported."""
    pass

class ValidationError(FileLoaderError):
    """Raised when data validation fails."""
    pass

# ============================================================================
# DECORATORS
# ============================================================================

def retry(max_attempts: int = 3, delay: float = 1.0):
    """Retry decorator for resilient operations."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_attempts - 1:
                        logger.warning(f"Attempt {attempt + 1} failed: {str(e)}. Retrying...")
                        time.sleep(delay * (attempt + 1))
            raise last_exception
        return wrapper
    return decorator

def monitor_performance(func):
    """Decorator to monitor memory and time usage."""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        
        if MEMORY_PROFILER_AVAILABLE:
            try:
                # memory_usage returns a list of memory values over time
                mem_before = memory_usage(-1, interval=0.1)[0]
                result_data = func(*args, **kwargs)
                mem_after = memory_usage(-1, interval=0.1)[0]
                mem_delta = max(0.0, mem_after - mem_before)
            except Exception as e:
                logger.debug(f"Memory profiling failed: {str(e)}")
                result_data = func(*args, **kwargs)
                mem_delta = 0.0
        else:
            result_data = func(*args, **kwargs)
            mem_delta = 0.0
        
        elapsed = time.time() - start_time
        
        # Ensure we don't break the return pattern
        if isinstance(result_data, tuple) and len(result_data) == 2:
            result, metadata = result_data
            if hasattr(metadata, 'load_time_seconds'):
                metadata.load_time_seconds = round(elapsed, 4)
            if hasattr(metadata, 'memory_usage_mb'):
                metadata.memory_usage_mb = round(mem_delta, 4)
            
            logger.info(
                f"{func.__name__} took {elapsed:.4f}s, "
                f"memory Δ: {mem_delta:.4f} MB"
            )
            return result, metadata
        
        return result_data, elapsed, mem_delta
    return wrapper

# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def compute_file_hash(file_path: Union[str, Path, BinaryIO], 
                     algorithm: str = 'md5') -> str:
    """Compute hash of file content."""
    hash_func = hashlib.new(algorithm)
    
    try:
        if isinstance(file_path, (str, Path)):
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(8192), b''):
                    hash_func.update(chunk)
        elif hasattr(file_path, 'read'):
            current_pos = file_path.tell()
            file_path.seek(0)
            for chunk in iter(lambda: file_path.read(8192), b''):
                hash_func.update(chunk)
            file_path.seek(current_pos)
    except Exception:
        return "unknown"
    
    return hash_func.hexdigest()

@contextmanager
def file_like_context(file_obj: Union[str, Path, BinaryIO, bytes]):
    """Context manager to handle different file input types."""
    temp_path = None
    f_handle = None
    try:
        if isinstance(file_obj, bytes):
            f_handle = io.BytesIO(file_obj)
            yield f_handle
        elif isinstance(file_obj, (str, Path)):
            yield file_obj
        elif hasattr(file_obj, 'read'):
            # For file-like objects, ensure we can seek
            if hasattr(file_obj, 'seek'):
                initial_pos = file_obj.tell()
                yield file_obj
                file_obj.seek(initial_pos)
            else:
                # If no seek, we MUST use a temp file
                with tempfile.NamedTemporaryFile(delete=False) as tmp:
                    tmp.write(file_obj.read())
                    temp_path = tmp.name
                yield temp_path
        else:
            raise FileLoaderError(f"Unsupported input type: {type(file_obj)}")
    finally:
        if temp_path and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except:
                pass
        if f_handle:
            try:
                f_handle.close()
            except:
                pass

def validate_dataframe(df: pd.DataFrame, safe_mode: bool = True) -> ValidationResult:
    """Standalone validation function."""
    return DataFrameValidator.validate(df, safe_mode)

# ============================================================================
# DETECTION FUNCTIONS
# ============================================================================

class FileDetector:
    """Detect file format, encoding, and compression."""
    
    # Magic bytes for format detection
    MAGIC_BYTES = {
        b'\x50\x4B\x03\x04': (FileFormat.EXCEL, 'zip/xlsx'),
        b'\xD0\xCF\x11\xE0': (FileFormat.EXCEL_OLD, 'ole2/xls'),
        b'PAR1': (FileFormat.PARQUET, 'parquet'),
        b'\x1A\x45\xDF\xA3': (FileFormat.UNKNOWN, 'matroska'),
    }
    
    # Compression signatures
    COMPRESSION_SIGNATURES = {
        b'\x1F\x8B\x08': CompressionFormat.GZIP,
        b'\x42\x5A\x68': CompressionFormat.BZIP2,
        b'\x50\x4B\x03\x04': CompressionFormat.ZIP,
        b'\xFD\x37\x7A\x58\x5A\x00': CompressionFormat.XZ,
        b'\x28\xB5\x2F\xFD': CompressionFormat.ZSTD,
    }
    
    @staticmethod
    def detect_format_and_compression(
        file_path: Union[str, Path, BinaryIO, bytes],
        sample_size: int = 4096
    ) -> Dict[str, Any]:
        """Detect file format and compression."""
        result = {
            'format': FileFormat.UNKNOWN,
            'format_name': None,
            'compression': CompressionFormat.NONE,
            'confidence': 0.0,
            'extension': None,
            'is_binary': False,
            'warnings': []
        }
        
        try:
            # Get sample bytes
            if isinstance(file_path, bytes):
                sample = file_path[:sample_size]
                extension = None
            elif isinstance(file_path, (str, Path)):
                with open(file_path, 'rb') as f:
                    sample = f.read(sample_size)
                extension = Path(file_path).suffix.lower()
                result['extension'] = extension
            elif hasattr(file_path, 'read'):
                current_pos = file_path.tell()
                file_path.seek(0)
                sample = file_path.read(sample_size)
                file_path.seek(current_pos)
                if hasattr(file_path, 'name'):
                    extension = Path(file_path.name).suffix.lower()
                    result['extension'] = extension
                else:
                    extension = None
            else:
                raise ValueError(f"Unsupported input type: {type(file_path)}")
            
            # Check for compression first
            for magic, comp_format in FileDetector.COMPRESSION_SIGNATURES.items():
                if sample.startswith(magic):
                    result['compression'] = comp_format
                    result['is_binary'] = True
                    break
            
            # Check for binary formats
            for magic, (format_enum, format_name) in FileDetector.MAGIC_BYTES.items():
                if sample.startswith(magic):
                    result['format'] = format_enum
                    result['format_name'] = format_name
                    result['confidence'] = 0.95
                    result['is_binary'] = True
                    break
            
            # If no binary magic, check if it's text
            if result['format'] == FileFormat.UNKNOWN:
                try:
                    sample.decode('utf-8')
                    result['is_binary'] = False
                    
                    # Check for common delimiters
                    delimiter_counts = {
                        ',': sample.count(b','),
                        '\t': sample.count(b'\t'),
                        ';': sample.count(b';'),
                        '|': sample.count(b'|')
                    }
                    
                    if max(delimiter_counts.values()) > 3:
                        result['format'] = FileFormat.CSV
                        result['confidence'] = 0.8
                    else:
                        # Check if it's JSON
                        try:
                            json.loads(sample.decode('utf-8')[:100])
                            result['format'] = FileFormat.JSON
                            result['confidence'] = 0.7
                        except:
                            result['format'] = FileFormat.UNKNOWN
                            result['confidence'] = 0.5
                            
                except UnicodeDecodeError:
                    result['is_binary'] = True
                    result['confidence'] = 0.5
            
            # Use extension as fallback
            if extension:
                extension_map = {
                    '.csv': FileFormat.CSV,
                    '.tsv': FileFormat.CSV,
                    '.txt': FileFormat.CSV,
                    '.xlsx': FileFormat.EXCEL,
                    '.xls': FileFormat.EXCEL_OLD,
                    '.xlsm': FileFormat.EXCEL,
                    '.parquet': FileFormat.PARQUET,
                    '.parq': FileFormat.PARQUET,
                    '.json': FileFormat.JSON,
                }
                
                if extension in extension_map:
                    format_from_ext = extension_map[extension]
                    if result['format'] == FileFormat.UNKNOWN:
                        result['format'] = format_from_ext
                        result['confidence'] = 0.7
                    elif result['format'] != format_from_ext:
                        result['warnings'].append(
                            f"Format mismatch: detected {result['format'].value}, "
                            f"but extension suggests {format_from_ext.value}"
                        )
            
            return result
            
        except Exception as e:
            logger.error(f"Error detecting format: {str(e)}")
            return {
                'format': FileFormat.UNKNOWN,
                'format_name': None,
                'compression': CompressionFormat.NONE,
                'confidence': 0.0,
                'extension': None,
                'is_binary': False,
                'warnings': [f"Detection error: {str(e)}"]
            }
    
    @staticmethod
    def detect_encoding(
        file_path: Union[str, Path, BinaryIO],
        sample_size: int = 10000
    ) -> Dict[str, Any]:
        """Detect file encoding."""
        try:
            if isinstance(file_path, (str, Path)):
                with open(file_path, 'rb') as f:
                    sample = f.read(sample_size)
            elif hasattr(file_path, 'read'):
                current_pos = file_path.tell()
                file_path.seek(0)
                sample = file_path.read(sample_size)
                file_path.seek(current_pos)
            else:
                return {
                    'encoding': 'utf-8',
                    'confidence': 0.0,
                    'language': '',
                    'sample_size': 0
                }
            
            result = chardet.detect(sample)
            
            return {
                'encoding': result['encoding'] if result['encoding'] else 'utf-8',
                'confidence': result['confidence'],
                'language': result.get('language', ''),
                'sample_size': len(sample)
            }
            
        except Exception as e:
            logger.warning(f"Failed to detect encoding: {str(e)}")
            return {
                'encoding': 'utf-8',
                'confidence': 0.0,
                'language': '',
                'sample_size': 0
            }
    
    @staticmethod
    def detect_csv_delimiter(
        file_path: Union[str, Path, BinaryIO],
        sample_lines: int = 20
    ) -> str:
        """Detect CSV delimiter using csv.Sniffer with robust fallbacks."""
        try:
            # 1. Try to get sample text
            sample_text = ""
            if isinstance(file_path, (str, Path)):
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    sample_text = "".join([f.readline() for _ in range(sample_lines)])
            elif hasattr(file_path, 'read'):
                current_pos = file_path.tell()
                file_path.seek(0)
                # Read as bytes and decode
                content = file_path.read(8192)
                if isinstance(content, bytes):
                    sample_text = content.decode('utf-8', errors='ignore')
                else:
                    sample_text = content
                file_path.seek(current_pos)
            
            if not sample_text:
                return ','
                
            # 2. Try csv.Sniffer (Industry Standard)
            try:
                sniffer = csv.Sniffer()
                # Determine likely delimiters to check
                dialect = sniffer.sniff(sample_text, delimiters=[',', '\t', ';', '|'])
                return dialect.delimiter
            except:
                # 3. Fallback to frequency-variance analysis (custom logic)
                delimiters = [',', '\t', ';', '|']
                lines = sample_text.splitlines()
                if not lines: return ','
                
                scores = {}
                for d in delimiters:
                    counts = [line.count(d) for line in lines if line.strip()]
                    if not counts or sum(counts) == 0:
                        continue
                    
                    avg = sum(counts) / len(counts)
                    # We want high average and low variance
                    variance = sum((c - avg) ** 2 for c in counts) / len(counts)
                    # Score: high frequency / (1 + variance)
                    scores[d] = avg / (1 + variance)
                
                if scores:
                    return max(scores.items(), key=lambda x: x[1])[0]
            
            return ','
            
        except Exception as e:
            logger.warning(f"Failed to detect delimiter: {str(e)}")
            return ','

# ============================================================================
# LOADER CLASSES
# ============================================================================

class BaseLoader:
    """Base class for all loaders."""
    
    @staticmethod
    def validate_file_size(
        file_path: Union[str, Path, BinaryIO],
        max_size_mb: Optional[float] = None
    ) -> Dict[str, Any]:
        """Validate file size."""
        try:
            if isinstance(file_path, (str, Path)):
                size_bytes = Path(file_path).stat().st_size
            elif hasattr(file_path, 'size'):
                size_bytes = file_path.size
            elif hasattr(file_path, 'getbuffer'):
                size_bytes = len(file_path.getbuffer())
            else:
                size_bytes = 0
            
            size_mb = size_bytes / (1024 * 1024)
            
            result = {
                'size_bytes': size_bytes,
                'size_mb': round(size_mb, 2),
                'is_within_limit': True,
                'warnings': []
            }
            
            if max_size_mb and size_mb > max_size_mb:
                result['is_within_limit'] = False
                result['warnings'].append(
                    f"File size ({size_mb:.2f} MB) exceeds limit ({max_size_mb} MB)"
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Error validating file size: {str(e)}")
            return {
                'size_bytes': 0,
                'size_mb': 0.0,
                'is_within_limit': False,
                'warnings': [f"Failed to get file size: {str(e)}"]
            }

class CSVLoader(BaseLoader):
    """Robust CSV loader."""
    
    @staticmethod
    @retry(max_attempts=2)
    def load(
        file_path: Union[str, Path, BinaryIO],
        encoding: Optional[str] = None,
        delimiter: Optional[str] = None,
        **kwargs
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Load CSV file."""
        metadata = {
            'format': FileFormat.CSV.value,
            'encoding': encoding,
            'delimiter': delimiter,
            'warnings': [],
            'errors': [],
            'strategies_tried': []
        }
        
        # Auto-detect encoding
        if encoding is None:
            encoding_info = FileDetector.detect_encoding(file_path)
            encoding = encoding_info['encoding']
            metadata['encoding_detected'] = encoding
            metadata['encoding_confidence'] = encoding_info['confidence']
        
        # Auto-detect delimiter
        if delimiter is None:
            delimiter = FileDetector.detect_csv_delimiter(file_path)
            metadata['delimiter_detected'] = delimiter
        
        # Reset file pointer
        if hasattr(file_path, 'seek'):
            file_path.seek(0)
        
        # Loading strategies
        strategies = [
            {'name': 'c_engine_fast', 'engine': 'c', 'low_memory': False, 'on_bad_lines': 'warn'},
            {'name': 'c_engine_safe', 'engine': 'c', 'on_bad_lines': 'warn'},
            {'name': 'python_engine', 'engine': 'python', 'on_bad_lines': 'warn'},
            {'name': 'python_engine_skip', 'engine': 'python', 'on_bad_lines': 'skip'}
        ]
        
        last_error = None
        
        for strategy in strategies:
            try:
                strategy_name = strategy.pop('name')
                metadata['strategies_tried'].append(strategy_name)
                
                strategy_kwargs = strategy.copy()
                strategy_kwargs.update({
                    'encoding': encoding,
                    'sep': delimiter,
                })
                strategy_kwargs.update(kwargs)
                
                if strategy_kwargs.get('engine') == 'python' and 'low_memory' in strategy_kwargs:
                    del strategy_kwargs['low_memory']
                
                logger.debug(f"Trying CSV strategy: {strategy_name}")
                
                if isinstance(file_path, (str, Path)):
                    df = pd.read_csv(file_path, **strategy_kwargs)
                else:
                    if hasattr(file_path, 'seek'):
                        file_path.seek(0)
                    df = pd.read_csv(file_path, **strategy_kwargs)
                
                metadata['success'] = True
                metadata['strategy_used'] = strategy_name
                metadata['shape'] = df.shape
                metadata['columns'] = list(df.columns)
                
                logger.info(f"CSV loaded with strategy: {strategy_name}, shape: {df.shape}")
                return df, metadata
                
            except Exception as e:
                last_error = e
                logger.debug(f"Strategy {strategy_name} failed: {str(e)}")
                continue
        
        # Manual fallback
        try:
            logger.warning("All strategies failed, trying manual parsing")
            
            if hasattr(file_path, 'seek'):
                file_path.seek(0)
                content = file_path.read()
                if isinstance(content, bytes):
                    content = content.decode(encoding or 'utf-8', errors='ignore')
            else:
                with open(file_path, 'r', encoding=encoding or 'utf-8', errors='ignore') as f:
                    content = f.read()
            
            lines = content.split('\n')
            reader = csv.reader(lines, delimiter=delimiter or ',')
            data = list(reader)
            
            if len(data) > 1:
                df = pd.DataFrame(data[1:], columns=data[0])
                metadata['success'] = True
                metadata['strategy_used'] = 'manual_parsing'
                metadata['shape'] = df.shape
                metadata['warnings'].append("Used manual parsing as fallback")
                return df, metadata
            else:
                raise FileLoaderError("No data found in file")
                
        except Exception as e:
            metadata['success'] = False
            metadata['errors'].append(f"All strategies failed. Last error: {str(last_error)}")
            raise FileLoaderError(f"Failed to load CSV. Last error: {str(last_error)}")

class ExcelLoader(BaseLoader):
    """Robust Excel loader."""
    
    @staticmethod
    @retry(max_attempts=2)
    def load(
        file_path: Union[str, Path, BinaryIO],
        sheet_name: Optional[Union[str, int, List]] = None,
        engine: Optional[str] = None,
        **kwargs
    ) -> Tuple[Union[pd.DataFrame, Dict[str, pd.DataFrame]], Dict[str, Any]]:
        """Load Excel file."""
        metadata = {
            'format': FileFormat.EXCEL.value,
            'sheet_name': sheet_name,
            'engine': engine,
            'available_sheets': [],
            'warnings': [],
            'errors': []
        }
        
        try:
            # Determine file extension for engine selection
            if isinstance(file_path, (str, Path)):
                filename = str(file_path)
            elif hasattr(file_path, 'name'):
                filename = file_path.name
            else:
                filename = ''
            
            # Auto-detect engine
            if engine is None:
                if '.xls' in filename.lower() and not filename.lower().endswith('.xlsx'):
                    engine = 'xlrd'
                else:
                    engine = 'openpyxl'
            
            metadata['engine'] = engine
            
            # Reset file pointer
            if hasattr(file_path, 'seek'):
                file_path.seek(0)
            
            # Get available sheet names
            if isinstance(file_path, (str, Path)):
                try:
                    xl = pd.ExcelFile(file_path, engine=engine)
                    metadata['available_sheets'] = xl.sheet_names
                    xl.close()
                except:
                    pass
            
            # Load Excel
            default_kwargs = {'engine': engine}
            default_kwargs.update(kwargs)
            
            if isinstance(file_path, (str, Path)):
                result = pd.read_excel(file_path, sheet_name=sheet_name, **default_kwargs)
            else:
                # File-like object - save to temp file
                with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp:
                    tmp.write(file_path.read())
                    tmp_path = tmp.name
                
                try:
                    result = pd.read_excel(tmp_path, sheet_name=sheet_name, **default_kwargs)
                finally:
                    try:
                        os.unlink(tmp_path)
                    except:
                        pass
            
            metadata['success'] = True
            
            # Handle results
            if isinstance(result, dict):
                metadata['loaded_sheets'] = list(result.keys())
                metadata['shape'] = {sheet: df.shape for sheet, df in result.items()}
                if sheet_name is not None and not isinstance(sheet_name, list):
                    result = list(result.values())[0]
            else:
                metadata['loaded_sheets'] = [sheet_name or 'Sheet1']
                metadata['shape'] = result.shape
            
            return result, metadata
            
        except Exception as e:
            metadata['success'] = False
            metadata['errors'].append(str(e))
            
            # Try fallback engine
            if engine != 'xlrd' and ('.xls' in filename.lower() and not filename.lower().endswith('.xlsx')):
                try:
                    logger.warning(f"Retrying with xlrd engine after error: {str(e)}")
                    return ExcelLoader.load(file_path, sheet_name=sheet_name, engine='xlrd', **kwargs)
                except:
                    pass
            
            raise FileLoaderError(f"Failed to load Excel file: {str(e)}")

class ParquetLoader(BaseLoader):
    """Robust Parquet loader."""
    
    @staticmethod
    @retry(max_attempts=2)
    def load(
        file_path: Union[str, Path, BinaryIO],
        engine: str = 'auto',
        **kwargs
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Load Parquet file."""
        metadata = {
            'format': FileFormat.PARQUET.value,
            'engine': engine,
            'warnings': [],
            'errors': []
        }
        
        try:
            # Reset file pointer
            if hasattr(file_path, 'seek'):
                file_path.seek(0)
            
            # Try pyarrow first
            if engine == 'auto' or engine == 'pyarrow':
                try:
                    df = pd.read_parquet(file_path, engine='pyarrow', **kwargs)
                    metadata['engine_used'] = 'pyarrow'
                    metadata['success'] = True
                    metadata['shape'] = df.shape
                    return df, metadata
                except Exception as e1:
                    if engine == 'pyarrow':
                        raise
                    logger.debug(f"PyArrow failed: {str(e1)}")
            
            # Try fastparquet as fallback
            try:
                if hasattr(file_path, 'seek'):
                    file_path.seek(0)
                df = pd.read_parquet(file_path, engine='fastparquet', **kwargs)
                metadata['engine_used'] = 'fastparquet'
                metadata['success'] = True
                metadata['shape'] = df.shape
                metadata['warnings'].append("Used fastparquet engine as fallback")
                return df, metadata
            except Exception as e2:
                logger.debug(f"FastParquet also failed: {str(e2)}")
                raise FileLoaderError(f"Both engines failed: {str(e1)}, {str(e2)}")
            
        except Exception as e:
            metadata['success'] = False
            metadata['errors'].append(str(e))
            raise FileLoaderError(f"Failed to load Parquet file: {str(e)}")

class JSONLoader(BaseLoader):
    """Robust JSON loader."""
    
    @staticmethod
    @retry(max_attempts=2)
    def load(
        file_path: Union[str, Path, BinaryIO],
        orient: str = 'auto',
        **kwargs
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Load JSON file."""
        metadata = {
            'format': FileFormat.JSON.value,
            'orient': orient,
            'warnings': [],
            'errors': []
        }
        
        try:
            # Reset file pointer
            if hasattr(file_path, 'seek'):
                file_path.seek(0)
            
            # Read content
            if isinstance(file_path, (str, Path)):
                with open(file_path, 'r', encoding='utf-8') as f:
                    content = f.read()
            else:
                content = file_path.read()
                if isinstance(content, bytes):
                    content = content.decode('utf-8')
            
            # Auto-detect orientation
            if orient == 'auto':
                try:
                    data = json.loads(content)
                    if isinstance(data, list):
                        orient = 'records'
                    elif isinstance(data, dict):
                        if 'columns' in data and 'data' in data:
                            orient = 'split'
                        elif 'schema' in data and 'data' in data:
                            orient = 'table'
                        else:
                            orient = 'columns'
                    else:
                        orient = 'records'
                    metadata['orient_detected'] = orient
                except:
                    orient = 'records'
                    metadata['warnings'].append("Could not auto-detect orientation")
            
            # Try different orientations
            orientations = [orient, 'records', 'columns', 'split', 'index']
            
            for try_orient in orientations:
                try:
                    if try_orient != orient:
                        metadata['warnings'].append(f"Trying orientation: {try_orient}")
                    
                    df = pd.read_json(io.StringIO(content), orient=try_orient, **kwargs)
                    metadata['success'] = True
                    metadata['orient_used'] = try_orient
                    metadata['shape'] = df.shape
                    return df, metadata
                except Exception as e:
                    if try_orient == orientations[-1]:
                        raise
                    continue
            
        except Exception as e:
            metadata['success'] = False
            metadata['errors'].append(str(e))
            raise FileLoaderError(f"Failed to load JSON file: {str(e)}")

# ============================================================================
# VALIDATION AND SANITIZATION
# ============================================================================

class DataFrameValidator:
    """Validate and sanitize DataFrames."""
    
    @staticmethod
    def validate(df: pd.DataFrame, safe_mode: bool = True) -> ValidationResult:
        """Validate DataFrame."""
        result = ValidationResult()
        
        try:
            # Basic shape validation
            if df.empty:
                result.errors.append("DataFrame is empty")
                if safe_mode:
                    result.critical_errors.append("Empty DataFrame")
                    result.is_valid = False
            
            # Column validation
            if len(df.columns) == 0:
                result.errors.append("No columns")
                result.critical_errors.append("No columns")
                result.is_valid = False
            
            # Check for duplicate columns
            duplicate_cols = df.columns[df.columns.duplicated()].tolist()
            if duplicate_cols:
                result.warnings.append(f"Duplicate columns: {duplicate_cols}")
            
            # Check suspicious column names
            suspicious_patterns = ['unnamed', 'unnamed:', 'blank', 'empty']
            suspicious_cols = [
                col for col in df.columns 
                if any(pattern in str(col).lower() for pattern in suspicious_patterns)
            ]
            if suspicious_cols:
                result.warnings.append(f"Suspicious columns: {suspicious_cols}")
            
            # Memory usage
            try:
                mem_usage = df.memory_usage(deep=True).sum() / (1024 * 1024)
                result.stats['memory_mb'] = round(mem_usage, 2)
                
                if mem_usage > 1000:
                    result.warnings.append(f"Large DataFrame: {mem_usage:.2f} MB")
            except:
                pass
            
            # Missing values
            try:
                missing_total = df.isna().sum().sum()
                missing_pct = (missing_total / (df.shape[0] * df.shape[1])) * 100
                result.stats['missing_total'] = missing_total
                result.stats['missing_pct'] = round(missing_pct, 2)
                
                if missing_pct > 50:
                    result.warnings.append(f"High missing values: {missing_pct:.2f}%")
            except:
                pass
            
            # Basic stats
            result.stats['rows'] = df.shape[0]
            result.stats['columns'] = df.shape[1]
            result.stats['dtypes'] = {
                str(dtype): count 
                for dtype, count in df.dtypes.value_counts().items()
            }
            
            return result
            
        except Exception as e:
            result.is_valid = False
            result.errors.append(f"Validation error: {str(e)}")
            result.critical_errors.append(f"Validation failed: {str(e)}")
            return result
    
    @staticmethod
    def sanitize(df: pd.DataFrame, config: Optional[Dict] = None) -> pd.DataFrame:
        """Sanitize DataFrame."""
        config = config or {}
        df_clean = df.copy()
        
        # Clean column names
        if config.get('clean_column_names', True):
            df_clean.columns = [
                str(col).strip().lower().replace(' ', '_').replace('-', '_')
                for col in df_clean.columns
            ]
        
        # Remove empty rows
        if config.get('drop_empty_rows', True):
            df_clean = df_clean.dropna(how='all')
        
        # Remove empty columns
        if config.get('drop_empty_cols', True):
            df_clean = df_clean.dropna(axis=1, how='all')
        
        # Reset index
        if config.get('reset_index', True):
            df_clean = df_clean.reset_index(drop=True)
        
        # Convert date columns
        date_columns = config.get('date_columns', [])
        for col in date_columns:
            if col in df_clean.columns:
                try:
                    df_clean[col] = pd.to_datetime(df_clean[col], errors='coerce')
                except:
                    pass
        
        # Remove duplicate rows
        if config.get('drop_duplicates', False):
            df_clean = df_clean.drop_duplicates()
        
        return df_clean

# ============================================================================
# MAIN LOADER CLASS
# ============================================================================

class EnhancedDataLoader:
    """
    Enhanced universal data file loader with intelligent auto-detection.
    
    Features:
    - Supports CSV, Excel, Parquet, JSON formats
    - Automatic format detection
    - Robust error handling with fallbacks
    - Size validation
    - Performance monitoring
    - DataFrame validation and sanitization
    - Streamlit integration
    """
    
    def __init__(
        self,
        max_size_mb: Optional[float] = None,
        safe_mode: bool = True,
        verbose: bool = False,
        auto_sanitize: bool = False,
        sanitize_config: Optional[Dict] = None
    ):
        """
        Initialize the data loader.
        
        Args:
            max_size_mb: Maximum file size in MB
            safe_mode: Enable safety checks and validations
            verbose: Print detailed information
            auto_sanitize: Automatically sanitize loaded DataFrames
            sanitize_config: Configuration for sanitization
        """
        self.max_size_mb = max_size_mb
        self.safe_mode = safe_mode
        self.verbose = verbose
        self.auto_sanitize = auto_sanitize
        self.sanitize_config = sanitize_config or {}
        
        # Initialize sub-loaders
        self.csv_loader = CSVLoader()
        self.excel_loader = ExcelLoader()
        self.parquet_loader = ParquetLoader()
        self.json_loader = JSONLoader()
        self.validator = DataFrameValidator()
    
    @monitor_performance
    def load(
        self,
        file_path: Union[str, Path, BinaryIO, bytes],
        file_format: Union[str, FileFormat] = FileFormat.AUTO,
        encoding: Optional[str] = None,
        delimiter: Optional[str] = None,
        sheet_name: Optional[Union[str, int, List]] = None,
        **kwargs
    ) -> Tuple[Union[pd.DataFrame, Dict[str, pd.DataFrame]], FileMetadata]:
        """
        Load data from file with intelligent auto-detection.
        
        Args:
            file_path: Path to file, file-like object, or bytes
            file_format: File format or 'auto' for detection
            encoding: File encoding (for text files)
            delimiter: CSV delimiter
            sheet_name: Excel sheet name(s)
            **kwargs: Format-specific parameters
            
        Returns:
            Tuple of (DataFrame(s), metadata)
            
        Raises:
            FileLoaderError: If loading fails
            FileTooLargeError: If file exceeds size limit
            InvalidFormatError: If format is invalid
        """
        start_time = time.time()
        
        # Convert string format to enum
        if isinstance(file_format, str):
            try:
                file_format = FileFormat(file_format.lower())
            except ValueError:
                file_format = FileFormat.AUTO
        
        # Initialize metadata
        orig_name = "data"
        if isinstance(file_path, (str, Path)):
            orig_name = Path(file_path).name
        elif hasattr(file_path, 'name'):
            orig_name = file_path.name
            
        metadata = FileMetadata(
            file_path=str(file_path) if isinstance(file_path, (str, Path)) else 'file_like_object',
            original_filename=orig_name,
            format_detected=FileFormat.UNKNOWN,
            format_used=FileFormat.UNKNOWN,
            confidence=0.0
        )
        
        try:
            # Handle bytes input
            if isinstance(file_path, bytes):
                file_like = io.BytesIO(file_path)
                file_path = file_like
                metadata.file_path = 'bytes_input'
                metadata.original_filename = 'bytes_data'
            
            # Validate file size
            size_info = BaseLoader.validate_file_size(file_path, self.max_size_mb)
            metadata.size_bytes = size_info['size_bytes']
            metadata.size_mb = size_info['size_mb']
            
            if not size_info['is_within_limit'] and self.max_size_mb:
                if self.safe_mode:
                    raise FileTooLargeError(
                        f"File size ({metadata.size_mb} MB) exceeds limit ({self.max_size_mb} MB)"
                    )
                else:
                    metadata.warnings.extend(size_info['warnings'])
            
            # Detect file format
            detection_result = FileDetector.detect_format_and_compression(file_path)
            metadata.format_detected = detection_result['format']
            metadata.confidence = detection_result['confidence']
            metadata.compression = detection_result['compression']
            metadata.warnings.extend(detection_result['warnings'])
            
            # Determine format to use
            if file_format != FileFormat.AUTO:
                metadata.format_used = file_format
                metadata.warnings.append(f"Using user-specified format: {file_format.value}")
            else:
                metadata.format_used = metadata.format_detected
            
            if self.verbose:
                logger.info(f"Detected format: {metadata.format_detected.value} (confidence: {metadata.confidence:.2f})")
                logger.info(f"Using format: {metadata.format_used.value}")
            
            # Load data based on format
            format_to_use = metadata.format_used
            
            if format_to_use == FileFormat.CSV:
                # Auto-detect delimiter if needed
                if delimiter is None and metadata.format_detected == FileFormat.CSV:
                    delimiter = FileDetector.detect_csv_delimiter(file_path)
                    if self.verbose:
                        logger.info(f"Detected delimiter: {repr(delimiter)}")
                
                df, format_metadata = self.csv_loader.load(
                    file_path,
                    encoding=encoding,
                    delimiter=delimiter,
                    **kwargs
                )
                metadata.encoding = format_metadata.get('encoding')
                metadata.delimiter = format_metadata.get('delimiter')
                
            elif format_to_use in [FileFormat.EXCEL, FileFormat.EXCEL_OLD]:
                df, format_metadata = self.excel_loader.load(
                    file_path,
                    sheet_name=sheet_name,
                    **kwargs
                )
                metadata.sheets_loaded = format_metadata.get('loaded_sheets', [])
                
            elif format_to_use == FileFormat.PARQUET:
                df, format_metadata = self.parquet_loader.load(
                    file_path,
                    **kwargs
                )
                
            elif format_to_use == FileFormat.JSON:
                df, format_metadata = self.json_loader.load(
                    file_path,
                    orient=kwargs.pop('orient', 'auto'),
                    **kwargs
                )
                
            else:
                # Try all formats as fallback
                if format_to_use == FileFormat.UNKNOWN:
                    logger.warning("Could not detect format, trying all formats")
                    
                    formats_to_try = [
                        (FileFormat.CSV, self.csv_loader),
                        (FileFormat.EXCEL, self.excel_loader),
                        (FileFormat.PARQUET, self.parquet_loader),
                        (FileFormat.JSON, self.json_loader),
                    ]
                    
                    last_error = None
                    
                    for fmt, loader in formats_to_try:
                        try:
                            if fmt == FileFormat.CSV:
                                df, format_metadata = loader.load(
                                    file_path,
                                    encoding=encoding,
                                    delimiter=delimiter,
                                    **kwargs
                                )
                            elif fmt == FileFormat.EXCEL:
                                df, format_metadata = loader.load(
                                    file_path,
                                    sheet_name=sheet_name,
                                    **kwargs
                                )
                            else:
                                df, format_metadata = loader.load(file_path, **kwargs)
                            
                            metadata.format_used = fmt
                            metadata.warnings.append(f"Auto-detected format as: {fmt.value}")
                            break
                        except Exception as e:
                            last_error = e
                            if fmt == formats_to_try[-1][0]:
                                raise FileLoaderError(
                                    f"Failed to load with any format. Last error: {str(last_error)}"
                                )
                            continue
                else:
                    raise InvalidFormatError(
                        f"Unsupported format: {format_to_use.value}. "
                        f"Supported: csv, excel, parquet, json"
                    )
            
            # Update metadata from format-specific loader
            if 'shape' in format_metadata:
                metadata.shape = format_metadata['shape']
            
            if 'columns' in format_metadata:
                metadata.columns = format_metadata['columns']
            
            if 'errors' in format_metadata:
                metadata.errors.extend(format_metadata['errors'])
            
            if 'warnings' in format_metadata:
                metadata.warnings.extend(format_metadata['warnings'])
            
            # Validate DataFrame(s)
            if isinstance(df, pd.DataFrame):
                validation = self.validator.validate(df, self.safe_mode)
                metadata.warnings.extend(validation.warnings)
                metadata.errors.extend(validation.errors)
                
                if not validation.is_valid and self.safe_mode and validation.critical_errors:
                    raise ValidationError(
                        f"Data validation failed: {validation.critical_errors[0]}"
                    )
                
                metadata.dtypes = validation.stats.get('dtypes', {})
                
                # Sanitize if enabled
                if self.auto_sanitize:
                    df = self.validator.sanitize(df, self.sanitize_config)
                    metadata.warnings.append("DataFrame was automatically sanitized")
                
            elif isinstance(df, dict):
                # Multiple DataFrames (Excel sheets)
                validation_results = {}
                for sheet_name, sheet_df in df.items():
                    validation = self.validator.validate(sheet_df, self.safe_mode)
                    validation_results[sheet_name] = validation
                    
                    if not validation.is_valid and self.safe_mode and validation.critical_errors:
                        raise ValidationError(
                            f"Sheet '{sheet_name}' validation failed: {validation.critical_errors[0]}"
                        )
                    
                    # Sanitize if enabled
                    if self.auto_sanitize:
                        df[sheet_name] = self.validator.sanitize(sheet_df, self.sanitize_config)
                
                metadata.warnings.append(f"Multiple sheets loaded: {list(df.keys())}")
            
            # Compute file hash
            try:
                metadata.hash_md5 = compute_file_hash(file_path)
            except:
                pass
            
            # Set load time
            metadata.load_time_seconds = round(time.time() - start_time, 2)
            
            if self.verbose:
                if isinstance(df, pd.DataFrame):
                    logger.info(f"Loaded DataFrame: {metadata.shape} in {metadata.load_time_seconds}s")
                elif isinstance(df, dict):
                    shapes = {k: v.shape for k, v in df.items()}
                    logger.info(f"Loaded multiple DataFrames: {shapes} in {metadata.load_time_seconds}s")
            
            return df, metadata
            
        except (FileTooLargeError, InvalidFormatError, ValidationError):
            raise
        except Exception as e:
            metadata.errors.append(str(e))
            metadata.load_time_seconds = round(time.time() - start_time, 2)
            
            if self.verbose:
                logger.error(f"Failed to load data: {str(e)}")
            
            raise FileLoaderError(f"Failed to load data: {str(e)}")
    
    def load_chunked(
        self,
        file_path: Union[str, Path],
        chunksize: int = 10000,
        file_format: Union[str, FileFormat] = FileFormat.AUTO,
        **kwargs
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Load data in chunks for memory efficiency.
        
        Args:
            file_path: Path to file
            chunksize: Number of rows per chunk
            file_format: File format or 'auto' for detection
            **kwargs: Format-specific parameters
            
        Yields:
            DataFrame chunks
            
        Note: Currently only supports CSV files.
        """
        if isinstance(file_format, str):
            file_format = FileFormat(file_format.lower())
        
        if file_format == FileFormat.AUTO:
            detection_result = FileDetector.detect_format_and_compression(file_path)
            file_format = detection_result['format']
        
        if file_format != FileFormat.CSV:
            raise NotImplementedError("Chunked loading only supported for CSV files")
        
        # Auto-detect delimiter and encoding
        delimiter = FileDetector.detect_csv_delimiter(file_path)
        encoding_info = FileDetector.detect_encoding(file_path)
        encoding = encoding_info['encoding']
        
        # Load in chunks
        for chunk in pd.read_csv(
            file_path,
            chunksize=chunksize,
            delimiter=delimiter,
            encoding=encoding,
            **kwargs
        ):
            if self.auto_sanitize:
                chunk = self.validator.sanitize(chunk, self.sanitize_config)
            yield chunk
    
    def load_directory(
        self,
        directory_path: Union[str, Path],
        pattern: str = "*",
        recursive: bool = False,
        **kwargs
    ) -> Dict[str, Tuple[pd.DataFrame, FileMetadata]]:
        """
        Load all matching files in a directory.
        
        Args:
            directory_path: Path to directory
            pattern: Glob pattern for file matching
            recursive: Search recursively
            **kwargs: Arguments passed to load()
            
        Returns:
            Dictionary mapping filenames to (DataFrame, metadata) tuples
        """
        directory = Path(directory_path)
        
        if recursive:
            files = list(directory.rglob(pattern))
        else:
            files = list(directory.glob(pattern))
        
        results = {}
        
        for file_path in files:
            if file_path.is_file():
                try:
                    df, metadata = self.load(file_path, **kwargs)
                    results[str(file_path)] = (df, metadata)
                except Exception as e:
                    logger.error(f"Failed to load {file_path}: {str(e)}")
                    continue
        
        return results

# ============================================================================
# STREAMLIT INTEGRATION
# ============================================================================

if STREAMLIT_AVAILABLE:
    
    class StreamlitDataLoader:
        """Streamlit wrapper for EnhancedDataLoader."""
        
        @staticmethod
        @st.cache_data(show_spinner="Loading data...", persist=True)
        def cached_load(_loader: EnhancedDataLoader, file_path, **kwargs):
            """Cached version of load for Streamlit."""
            return _loader.load(file_path, **kwargs)
        
        @staticmethod
        def file_uploader(
            label: str = "Upload a file",
            formats: Optional[List[str]] = None,
            max_size_mb: float = 100,
            key: Optional[str] = None,
            **loader_kwargs
        ) -> Optional[Tuple[pd.DataFrame, FileMetadata]]:
            """
            Streamlit file uploader with integrated loading.
            
            Args:
                label: Uploader label
                formats: List of accepted file extensions
                max_size_mb: Maximum file size
                key: Streamlit widget key
                **loader_kwargs: Arguments for EnhancedDataLoader
            
            Returns:
                Tuple of (DataFrame, metadata) or None
            """
            if formats is None:
                formats = [".csv", ".xlsx", ".xls", ".parquet", ".json"]
            
            uploaded_file = st.file_uploader(
                label,
                type=formats,
                key=key
            )
            
            if uploaded_file is not None:
                # Create loader
                loader = EnhancedDataLoader(
                    max_size_mb=max_size_mb,
                    **loader_kwargs
                )
                
                try:
                    # Load file
                    df, metadata = StreamlitDataLoader.cached_load(
                        loader,
                        uploaded_file,
                        verbose=True
                    )
                    
                    # Display metadata in expander
                    with st.expander("📄 File Information", expanded=False):
                        st.json(metadata.to_dict())
                    
                    # Display preview
                    with st.expander("👀 Data Preview", expanded=True):
                        if isinstance(df, pd.DataFrame):
                            st.dataframe(df.head(100), use_container_width=True)
                            st.caption(f"Shape: {df.shape[0]} rows × {df.shape[1]} columns")
                        elif isinstance(df, dict):
                            selected_sheet = st.selectbox(
                                "Select sheet to preview",
                                list(df.keys())
                            )
                            st.dataframe(df[selected_sheet].head(100), use_container_width=True)
                            st.caption(f"Shape: {df[selected_sheet].shape[0]} rows × {df[selected_sheet].shape[1]} columns")
                    
                    return df, metadata
                    
                except FileLoaderError as e:
                    st.error(f"❌ Error loading file: {str(e)}")
                    return None
            
            return None

# ============================================================================
# CONVENIENCE FUNCTIONS
# ============================================================================

def quick_load(
    file_path: Union[str, Path, BinaryIO, bytes],
    **kwargs
) -> pd.DataFrame:
    """
    Quick load function for simple use cases.
    
    Args:
        file_path: Path to file or file-like object
        **kwargs: Arguments for EnhancedDataLoader.load()
    
    Returns:
        Loaded DataFrame
    
    Example:
        df = quick_load("data.csv")
        df = quick_load(uploaded_file)  # In Streamlit
    """
    loader = EnhancedDataLoader(verbose=False)
    df, _ = loader.load(file_path, **kwargs)
    
    if isinstance(df, dict):
        # Return first sheet for Excel files
        df = list(df.values())[0]
    
    return df

def load_and_validate(
    file_path: Union[str, Path, BinaryIO, bytes],
    validation_rules: Optional[Dict] = None,
    **kwargs
) -> Tuple[pd.DataFrame, FileMetadata, ValidationResult]:
    """
    Load file and return validation results.
    
    Args:
        file_path: Path to file or file-like object
        validation_rules: Custom validation rules
        **kwargs: Arguments for EnhancedDataLoader
    
    Returns:
        Tuple of (DataFrame, metadata, validation_results)
    """
    loader = EnhancedDataLoader(**kwargs)
    validator = DataFrameValidator()
    
    df, metadata = loader.load(file_path)
    
    if isinstance(df, dict):
        # Validate first sheet only
        first_key = list(df.keys())[0]
        validation = validator.validate(df[first_key])
    else:
        validation = validator.validate(df)
    
    return df, metadata, validation

# ============================================================================
# MAIN USAGE EXAMPLES
# ============================================================================

def main():
    """Example usage of the EnhancedDataLoader."""
    
    # Save example files first if they don't exist
    example_data = pd.DataFrame({
        'id': range(1, 101),
        'name': [f'Name_{i}' for i in range(1, 101)],
        'value': np.random.randn(100),
        'date': pd.date_range('2023-01-01', periods=100),
        'category': np.random.choice(['A', 'B', 'C'], 100)
    })
    
    example_data.to_csv("data.csv", index=False)
    example_data.to_excel("data.xlsx", index=False)
    example_data.to_parquet("data.parquet", index=False)
    example_data.to_json("data.json", orient='records')
    
    print("Created example data files: data.csv, data.xlsx, data.parquet, data.json")
    print("=" * 60)

    # Example 1: Basic usage
    print("Example 1: Basic usage")
    try:
        loader = EnhancedDataLoader(max_size_mb=10, verbose=True)
        df, metadata = loader.load("data.csv")
        print(f"Loaded {metadata.shape} DataFrame in {metadata.load_time_seconds}s")
        print(f"Columns: {metadata.columns[:5]}...")
    except FileLoaderError as e:
        print(f"Error: {e}")
    
    # Example 2: Auto-sanitize
    print("\nExample 2: Auto-sanitize")
    try:
        loader = EnhancedDataLoader(auto_sanitize=True, sanitize_config={
            'clean_column_names': True,
            'drop_empty_rows': True,
            'date_columns': ['date']
        })
        df, metadata = loader.load("data.csv")
        print(f"Sanitized DataFrame shape: {df.shape}")
    except Exception as e:
        print(f"Sanitization error: {e}")
    
    # Example 3: Chunked loading
    print("\nExample 3: Chunked loading")
    try:
        loader = EnhancedDataLoader()
        # Use the created data.csv instead of non-existent large_data.csv
        for i, chunk in enumerate(loader.load_chunked("data.csv", chunksize=20)):
            print(f"Processing chunk {i+1}: {chunk.shape}")
            if i >= 2:
                break
    except Exception as e:
        print(f"Chunked loading error: {e}")
    
    # Example 4: Directory loading
    print("\nExample 4: Directory loading")
    try:
        loader = EnhancedDataLoader()
        results = loader.load_directory(".", pattern="data.*")
        print(f"Loaded {len(results)} files from current directory")
        for filename, res in results.items():
            df_item, meta_item = res
            print(f"  {os.path.basename(filename)}: {meta_item.shape}")
    except Exception as e:
        print(f"Directory loading error: {e}")
    
    # Example 5: Quick load function
    print("\nExample 5: Quick load")
    try:
        df = quick_load("data.csv", delimiter=",")
        print(f"Quick loaded shape: {df.shape}")
    except Exception as e:
        print(f"Quick load error: {e}")
    
    # Example 6: With validation
    print("\nExample 6: Load with validation")
    try:
        df, metadata, validation = load_and_validate(
            "data.csv",
            safe_mode=True
        )
        print(f"Is valid: {validation.is_valid}")
        print(f"Warnings: {validation.warnings[:3]}")
    except Exception as e:
        print(f"Validation error: {e}")

    # Clean up
    for ext in ['.csv', '.xlsx', '.parquet', '.json']:
        try:
            os.remove(f"data{ext}")
        except:
            pass

if __name__ == "__main__":
    # Configure logging for examples
    logging.basicConfig(level=logging.INFO)
    
    # Create example data files if they don't exist
    example_data = pd.DataFrame({
        'id': range(1, 101),
        'name': [f'Name_{i}' for i in range(1, 101)],
        'value': np.random.randn(100),
        'date': pd.date_range('2023-01-01', periods=100),
        'category': np.random.choice(['A', 'B', 'C'], 100)
    })
    
    # Save example files
    example_data.to_csv("data.csv", index=False)
    example_data.to_excel("data.xlsx", index=False)
    example_data.to_parquet("data.parquet", index=False)
    example_data.to_json("data.json", orient='records')
    
    print("Created example data files: data.csv, data.xlsx, data.parquet, data.json")
    print("=" * 60)
    
    # Run examples
    main()
    
    # Clean up example files
    for ext in ['.csv', '.xlsx', '.parquet', '.json']:
        try:
            os.remove(f"data{ext}")
        except:
            pass