import time
from test import TestParams, simple_test
from router import get_bytes_sent, reset_bytes_sent

benchmark_params = {
    "Original": TestParams(P=7, A=7, L=7, f=1, t=1),
    "Parameterized": TestParams(P=7, A=7, L=7, f=2, t=0),
    "OriginalLarge": TestParams(P=21, A=21, L=21, f=4, t=4),
    "ParameterizedLarge": TestParams(P=21, A=21, L=21, f=6, t=1)
}

for name, params in benchmark_params.items():
    timings = []
    reset_bytes_sent()

    for _ in range(1):
        t1 = time.perf_counter()
        simple_test(ommission=True, test_params=params)
        t2 = time.perf_counter()
        timings.append(t2 - t1)
    
    print(f"Simple test for {name} algorithm took {sum(timings) / len(timings)}s")
    print(f"{get_bytes_sent() / len(timings)} bytes sent in total")