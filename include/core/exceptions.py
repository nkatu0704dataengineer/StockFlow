"""
StockFlow Core Exceptions.

This module defines common exceptions raised across the StockFlow data platform.
"""

class ProviderException(Exception):
    """
    Base exception class for all data providers.
    
    Any exception raised during external provider communication or response
    validation should inherit from this class.
    """
    pass


class ProviderAdapterException(ProviderException):
    """
    Exception raised when the provider adapter cannot satisfy a fetch request
    after its configured provider fallback and retry policy is exhausted.
    """
    pass


class NormalizerException(Exception):
    """
    Exception raised when a provider payload cannot be normalized into the
    StockFlow unified quote schema.
    """
    pass


class SchemaValidationException(Exception):
    """
    Exception raised when a StockFlow schema object fails its required
    structural validation rules.
    """
    pass


class FinnhubProviderException(ProviderException):
    """
    Exception raised when Finnhub provider operations fail.
    """
    pass


class YahooProviderException(ProviderException):
    """
    Exception raised when Yahoo Finance provider operations fail.
    """
    pass


class BronzeReaderException(Exception):
    """
    Exception raised when the Bronze reader cannot convert the provided
    Python object into a Spark DataFrame.
    """
    pass


class BronzeValidatorException(Exception):
    """
    Exception raised when the Bronze validator detects a structural or
    quality issue in a Spark DataFrame before it is persisted.
    """
    pass


class BronzeWriterException(Exception):
    """
    Exception raised when the Bronze writer cannot persist a validated
    Spark DataFrame to the requested destination format.
    """
    pass


class BronzeMetadataException(Exception):
    """
    Exception raised when Bronze metadata enrichment cannot be performed on
    the supplied Spark DataFrame.
    """
    pass


class BronzePipelineException(Exception):
    """
    Exception raised when the Bronze pipeline orchestration fails while
    sequencing the reader, validator, metadata, and writer components.
    """
    pass


class BronzeJobException(Exception):
    """
    Exception raised when the Bronze job cannot complete its orchestration
    sequence for the configured symbol set.
    """
    pass
