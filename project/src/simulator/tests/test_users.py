"""Pool de usuarios por fuente (§3.8, HU-014)."""

import random
import re
from collections import Counter

from simulator.enrich.users import UserPool
from simulator.tests._sim_helpers import config_with, factory


def test_pool_is_stable_and_independent_of_sim_seed():
    a = factory(7).pool("reddit")
    b = UserPool("reddit", config_with(env={"SIM_SEED": "99"}))
    assert len(a) == 2000
    assert a.users == b.users  # mismo pool en stream (seed 7) e histórico (otra semilla)


def test_ids_unique_across_sources_and_handles_by_network():
    f = factory()
    ids = set()
    for source, pattern in (("reddit", r"^u/\S+$"), ("instagram", r"^@[a-z0-9._]+$"), ("facebook", r"^\S+ \S+")):
        pool = f.pool(source)
        for u in pool.users:
            assert re.match(r"^u_\d{6}$", u.user_id)
            assert re.match(pattern, u.handle), (source, u.handle)
            assert u.followers >= 0
            ids.add(u.user_id)
        if source != "facebook":
            assert len({u.handle for u in pool.users}) == len(pool)
    assert len(ids) == 6000


def test_zipf_activity():
    pool = factory().pool("instagram")
    rng = random.Random(5)
    counts = Counter(pool.pick(rng).user_id for _ in range(20000))
    top = sum(c for _, c in counts.most_common(200))  # 10 % de los usuarios
    assert top / 20000 > 0.4


def test_followers_are_lognormal_like():
    f = sorted(u.followers for u in factory().pool("facebook").users)
    median, p99 = f[len(f) // 2], f[int(len(f) * 0.99)]
    assert 50 < median < 400 and p99 > 5 * median
