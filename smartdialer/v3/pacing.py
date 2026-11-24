from dataclasses import dataclass


@dataclass(frozen=True)
class PacingInputs:
    free_agents: int
    batch_limit: int


@dataclass(frozen=True)
class PacingProposal:
    requested_calls: int
    inputs: PacingInputs
    reasoning: str


def propose(inputs: PacingInputs) -> PacingProposal:
    requested = max(0, min(inputs.free_agents, inputs.batch_limit))
    return PacingProposal(
        requested, inputs,
        f"free={inputs.free_agents}, batch_limit={inputs.batch_limit}, request={requested}",
    )
