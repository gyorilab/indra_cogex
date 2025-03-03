#!/usr/bin/env python3
import argparse
import logging
from datetime import datetime

from indra_cogex.analysis.source_targets_explanation import explain_downstream

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
        args.output_dir = f'analysis_results_{timestamp}'

    try:
        # Run the analysis using the existing explain_downstream function
        explain_downstream(
            args.source,
            args.targets,
            args.output_dir,
            id_type=args.id_type
        )
        logger.info(f'Analysis results saved to {args.output_dir}')

    except Exception as e:
        logger.error(f'Analysis failed: {str(e)}')
        raise


if __name__ == '__main__':
    main()
