# src/run.py
from src.data_prep import DataProcessor
from src.data_prep_seq import create_spatial_sequences
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PipelineRunner")

def main():
    logger.info("=== Step 1: Tabular Data Preprocessing ===")
    processor = DataProcessor()
    processor.prepare_tabular()
    logger.info("Tabular preprocessing done!")

    logger.info("=== Step 2: Creating Spatial Sequences ===")
    dataloader = create_spatial_sequences(sample_ratio=0.05)
    logger.info(f"Spatial sequences created. Number of batches: {len(dataloader)}")

    # Example: inspect first batch
    for X, y in dataloader:
        logger.info(f"Batch X shape: {X.shape}, Batch y shape: {y.shape}")
        break

if __name__ == "__main__":
    main()