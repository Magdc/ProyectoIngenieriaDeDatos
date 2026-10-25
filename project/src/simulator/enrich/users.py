"""Pool estable de usuarios por fuente (§3.8, HU-014).

- ~2.000 usuarios por fuente, generados con ``Faker('es_CO')`` y una semilla FIJA
  (``users.pool_seed``), independiente de ``SIM_SEED``: el mismo pool sirve al stream
  y al histórico (Lote 3.1), así los usuarios se repiten entre orígenes.
- Actividad tipo Zipf: pocos usuarios publican mucho.
- Seguidores lognormales y estables por usuario.
- ``user_id`` = ``u_{n:06d}`` con un rango por fuente (reddit 0…, facebook N…,
  instagram 2N…), para que no haya dos personas distintas con el mismo id.
"""

from __future__ import annotations

import bisect
import random
import re
import unicodedata
from dataclasses import dataclass
from itertools import accumulate

from simulator.sim_config import SOURCES, SimConfig


@dataclass(frozen=True)
class User:
    user_id: str
    handle: str
    followers: int
    display_name: str


def _slug(text: str) -> str:
    plain = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode().lower()
    return re.sub(r"[^a-z0-9]+", "", plain)


def _handle(source: str, first: str, last: str, rng: random.Random) -> str:
    f, l = _slug(first) or "user", _slug(last) or "co"
    style = rng.randrange(4)
    n = rng.randint(1, 99)
    if source == "reddit":
        base = [f"{f}{l[:1]}_{n}", f"{f}_{l}", f"{l}{f[:3]}{n}", f"{f}{n:02d}"][style]
        return f"u/{base}"
    if source == "instagram":
        base = [f"{f}_{n:02d}", f"{f}.{l}", f"{f}{l}_{n}", f"{l}_{f}{n}"][style]
        return f"@{base}"
    # facebook: nombre visible
    return f"{first} {last}"


class UserPool:
    """Pool determinista de usuarios de una fuente."""

    def __init__(self, source: str, config: SimConfig) -> None:
        if source not in SOURCES:
            raise ValueError(f"fuente desconocida: {source!r}")
        from faker import Faker  # import diferido: solo se necesita al construir

        self.source = source
        size = config.pool_size
        offset = SOURCES.index(source) * size
        fake = Faker("es_CO")
        fake.seed_instance(f"{config.pool_seed}|users|{source}")
        rng = random.Random(f"{config.pool_seed}|users|{source}")
        users: list[User] = []
        seen: set[str] = set()
        for i in range(size):
            first, last = fake.first_name(), fake.last_name()
            handle = _handle(source, first, last, rng)
            while handle in seen and source != "facebook":
                handle = f"{handle}{rng.randint(0, 9)}"
            seen.add(handle)
            followers = int(rng.lognormvariate(config.followers_mu, config.followers_sigma))
            users.append(
                User(
                    user_id=f"u_{offset + i:06d}",
                    handle=handle,
                    followers=max(0, followers),
                    display_name=f"{first} {last}",
                )
            )
        # El orden de actividad (rango Zipf) se baraja para no ligarlo al índice.
        ranks = list(range(size))
        rng.shuffle(ranks)
        self.users: tuple[User, ...] = tuple(users[r] for r in ranks)
        weights = [1.0 / (k + 1) ** config.zipf_s for k in range(size)]
        self._cum = list(accumulate(weights))

    def __len__(self) -> int:
        return len(self.users)

    def pick(self, rng: random.Random) -> User:
        """Una extracción de ``rng``; los primeros del ranking salen mucho más."""
        x = rng.random() * self._cum[-1]
        return self.users[bisect.bisect_right(self._cum, x)]

    def by_id(self, user_id: str) -> User:
        for u in self.users:
            if u.user_id == user_id:
                return u
        raise KeyError(user_id)
