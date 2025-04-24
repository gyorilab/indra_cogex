#!/usr/bin/env python3
import argparse
import logging
from datetime import datetime
from pathlib import Path

from indra_cogex.analysis.source_targets_explanation import souce_target_analysis

logger = logging.getLogger(__name__)


def main():
    """Command line interface for running source-target analysis."""
    parser = argparse.ArgumentParser(
        description='Analyze mechanisms connecting source and target proteins using INDRA CoGEx.'
    )
    parser.add_argument('source',
                        help='Source gene symbol or HGNC ID')
    parser.add_argument('targets',
                        nargs='+',
                        help='Target gene symbols or HGNC IDs')
    parser.add_argument(
        '--id-type',
        choices=['hgnc.symbol', 'hgnc'],
        default='hgnc.symbol',
        help='Type of identifiers provided (default: hgnc.symbol)'
    )
    parser.add_argument(
        '--output-dir',
        default=None,
        help='Directory to save results (default: creates timestamped directory)'
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        help='Set the logging level (default: INFO)'
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # Create output directory if not specified
    if args.output_dir is None:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = Path(f'./analysis_results_{timestamp}')
    else:
        output_dir = Path(args.output_dir)

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    # Convert Path to string for compatibility with the function
    output_dir_str = str(output_dir)

    try:
        # Run the analysis with the output directory parameter
        result = souce_target_analysis(
            args.source,
            args.targets,
            output_dir=output_dir_str,  # Pass the output directory
            id_type=args.id_type
        )

        # Log where the results were saved
        logger.info(f'Analysis complete. Results saved to {output_dir}')

        # Log individual files that were created (if debug logging is enabled)
        if logger.isEnabledFor(logging.DEBUG):
            for item in output_dir.glob('**/*'):
                if item.is_file():
                    rel_path = item.relative_to(output_dir)
                    logger.debug(f'Created file: {rel_path}')

            # Provide a summary of what was generated
            logger.info(f'Generated {sum(1 for _ in output_dir.glob("**/*") if _.is_file())} files')

    except Exception as e:
        logger.error(f'Analysis failed: {str(e)}')
        raise


if __name__ == '__main__':
    main()