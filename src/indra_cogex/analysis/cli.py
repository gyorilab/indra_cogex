#!/usr/bin/env python3
import argparse
import logging
import pickle
from datetime import datetime
from pathlib import Path

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
        output_dir = Path(f'./analysis_results_{timestamp}')
    else:
        output_dir = Path(args.output_dir)

    try:
        # Run the analysis using the existing explain_downstream function
        result = explain_downstream(
            args.source,
            args.targets,
            id_type=args.id_type
        )
        # Save as pickle
        # todo: save individual results i.e. plots, tables, etc. as separate files
        output_dir.mkdir(parents=True, exist_ok=True)
        with (output_dir / 'analysis_results.pkl').open("wb") as f:
            pickle.dump(file=f, obj=result)
        logger.info(f'Analysis results saved to {output_dir}')

    except Exception as e:
        logger.error(f'Analysis failed: {str(e)}')
        raise


if __name__ == '__main__':
    main()
