from modal import Stub, Volume

stub = Stub("fastapi-demo")

vol = Volume.persisted("my-volume")