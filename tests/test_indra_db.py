from indra.belief import BeliefEngine
from indra.statements import Agent, Evidence, Activation


def test_unit_belief_calc():
    activation = Activation(
        Agent("A"),
        Agent("B"),
        evidence=[Evidence(source_api="reach") for _ in range(3)],
    )

    # Test that the belief score is calculated correctly
    assert activation.belief == 1

    # Set up default Belief Engine
    belief_engine = BeliefEngine()

    belief_engine.set_prior_probs([activation])

    assert activation.belief != 1
    assert activation.belief == 0.923
