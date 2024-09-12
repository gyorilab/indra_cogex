import requests
from flask import url_for
from app import app  # Import your Flask app


def test_discrete_analysis():
    with app.test_client() as client:
        with app.app_context():
            # Test the GET request
            response = client.get('/metabolite/discrete')
            assert response.status_code == 200

            # Test the POST request
            data = {
                'metabolites': 'CHEBI:17234, CHEBI:15377, CHEBI:16236, CHEBI:17351, CHEBI:18367',
                'minimum_evidence': 1,
                'minimum_belief': 0.8,
                'alpha': 0.05,
                'correction': 'bonferroni',
                'keep_insignificant': False,
                'submit': True
            }
            response = client.post('/metabolite/discrete', data=data, follow_redirects=True)
            assert response.status_code == 200

            # Check if the response contains expected content
            assert b'Results' in response.data
            assert b'CHEBI:17234' in response.data  # Check for Glucose
            assert b'CHEBI:15377' in response.data  # Check for Water

            print("Discrete analysis test passed successfully!")


def test_enzyme_route():
    with app.test_client() as client:
        with app.app_context():
            response = client.get('/metabolite/enzyme/1.1.1.1')
            assert response.status_code == 200

            # Check if the response contains expected content
            assert b'EC:1.1.1.1' in response.data
            assert b'Alcohol dehydrogenase' in response.data

            print("Enzyme route test passed successfully!")


if __name__ == '__main__':
    test_discrete_analysis()
    test_enzyme_route()
    print("All tests completed!")