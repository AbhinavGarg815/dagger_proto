#!/usr/bin/env python3
"""The behaviour half of the app <-> rover contract. Plays the app's part.

    python proto/contract_test.py --url ws://127.0.0.1:8082/api
    python proto/contract_test.py --url ws://192.168.144.100:<port>/api --allow-real-rover

Run against the fake rover (fake-rover/) and against the real gateway: the
Phase 1 exit test is that BOTH pass this file unchanged (docs/handoff.md
section 15). The byte-level half is proto/vectors.

WHAT IT SENDS — and nothing else: Hello, Heartbeat, and STOP. STOP is the one
command, and stopping is always safe, but CLAUDE.md says commands go to the
fake rover only unless the owner agrees — so any address other than this PC
needs --allow-real-rover. Point it at a real rover only when the rover is
parked and someone has the RC transmitter in hand.

It never assumes what the rover is doing, so it holds for any parked rover.
Behaviour that needs a moving rover (link-loss pause) is in
fake-rover/scene_test.py, against the fake rover only.

Needs grpcio-tools, protobuf and tornado (fake-rover/requirements.txt).
"""

import argparse
import asyncio
import os
import sys
import time
import urllib.parse
import uuid

import tornado.websocket

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from pyload import load_api  # noqa: E402

pb = load_api()


class Failed(Exception):
    pass


class Report:
    def __init__(self, label=""):
        self.passed = 0
        self.label = label

    def check(self, condition, what):
        if not condition:
            print(f"FAIL  {self.label}{what}", flush=True)
            raise Failed(what)
        self.passed += 1
        print(f"ok    {self.label}{what}", flush=True)


def mono_ms():
    return int(time.monotonic() * 1000)


class App:
    """The smallest possible app: sends envelopes, queues what comes back.

    A background reader puts every incoming envelope on a queue (stamped with
    its arrival time), so waiting for one kind of message never loses another.
    It echoes the rover's heartbeats like the real app, so the rover can
    measure the round trip.
    """

    def __init__(self, conn):
        self.conn = conn
        self.rover_id = ""
        self.inbox = asyncio.Queue()
        self.hb_seq = 0
        self.peer_hb = None        # (seq, sent_time_ms, arrived_ms)
        self.last_rtt_ms = None
        self.heartbeat_task = None
        self.tasks = [asyncio.ensure_future(self._read())]

    @classmethod
    async def connect(cls, url, attempts=50):
        for _ in range(attempts):
            try:
                return cls(await tornado.websocket.websocket_connect(url))
            except (ConnectionRefusedError, OSError):
                await asyncio.sleep(0.1)
        raise Failed(f"could not connect to {url}")

    async def _read(self):
        while True:
            raw = await self.conn.read_message()
            if raw is None:
                return
            arrived = mono_ms()
            env = pb.Envelope()
            env.ParseFromString(raw)
            if env.WhichOneof("body") == "heartbeat":
                hb = env.heartbeat
                self.peer_hb = (hb.seq, hb.sent_time_ms, arrived)
                if hb.echo_sent_time_ms:
                    self.last_rtt_ms = arrived - hb.echo_sent_time_ms - hb.echo_hold_ms
            await self.inbox.put(env)

    async def send(self, **body):
        env = pb.Envelope(rover_id=self.rover_id, sent_at_ms=int(time.time() * 1000), **body)
        await self.conn.write_message(env.SerializeToString(), binary=True)

    async def receive(self, kind, timeout=2.0):
        """The next envelope of this kind, discarding others. None on timeout."""
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            try:
                env = await asyncio.wait_for(self.inbox.get(), remaining)
            except asyncio.TimeoutError:
                return None
            if env.WhichOneof("body") == kind:
                return env

    async def collect(self, kind, seconds):
        found = []
        deadline = time.monotonic() + seconds
        while True:
            env = await self.receive(kind, deadline - time.monotonic())
            if env is None:
                return found
            found.append(env)

    def drain(self):
        while not self.inbox.empty():
            self.inbox.get_nowait()

    def hello(self, version=pb.PROTOCOL_VERSION_1):
        return self.send(hello=pb.Hello(protocol_version=version,
                                        app_version="contract-test", client_id="contract-test"))

    async def handshake(self):
        await self.hello()
        reply = await self.receive("hello_reply")
        if reply is not None and reply.hello_reply.accepted:
            self.rover_id = reply.hello_reply.rover_id
        return reply

    def start_heartbeats(self, period_s=0.25):
        async def beat():
            while True:
                self.hb_seq += 1
                hb = pb.Heartbeat(seq=self.hb_seq, sent_time_ms=mono_ms())
                if self.peer_hb:
                    seq, sent, arrived = self.peer_hb
                    hb.echo_seq = seq
                    hb.echo_sent_time_ms = sent
                    hb.echo_hold_ms = mono_ms() - arrived
                await self.send(heartbeat=hb)
                await asyncio.sleep(period_s)
        self.heartbeat_task = asyncio.ensure_future(beat())

    def stop_heartbeats(self):
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
            self.heartbeat_task = None

    def close(self):
        self.stop_heartbeats()
        for t in self.tasks:
            t.cancel()
        self.conn.close()


def stop_command():
    return pb.Command(command_id=str(uuid.uuid4()), stop=pb.Stop())


async def contract(url, r):
    # --- Rule 1: version handshake -----------------------------------------
    old = await App.connect(url)
    await old.hello(version=pb.PROTOCOL_VERSION_UNSPECIFIED)
    reply = await old.receive("hello_reply")
    r.check(reply is not None and not reply.hello_reply.accepted,
            "an unknown protocol version is refused (the app shows 'Update needed')")
    r.check(await old.receive("state", timeout=0.6) is None, "a refused app gets no state")
    old.close()

    app = await App.connect(url)
    try:
        await contract_main(app, r)
    finally:
        app.close()


async def contract_main(app, r):
    # Commands before the handshake are refused, not obeyed.
    await app.send(command=stop_command())
    res = await app.receive("command_result")
    r.check(res is not None
            and res.command_result.outcome == pb.COMMAND_OUTCOME_REJECTED
            and res.command_result.reject_reason == pb.REJECT_REASON_NO_HANDSHAKE,
            "a command before the handshake is rejected with NO_HANDSHAKE")

    reply = await app.handshake()
    r.check(reply is not None and reply.hello_reply.accepted, "protocol version 1 is accepted")
    hr = reply.hello_reply
    r.check(hr.protocol_version == pb.PROTOCOL_VERSION_1, "the rover says it speaks version 1")
    app.start_heartbeats()

    # --- Rules 4 and 7: capabilities and rover id --------------------------
    r.check(pb.CAPABILITY_STOP in hr.capabilities, "the rover advertises STOP")
    r.check(bool(hr.rover_id), f"the rover identifies itself ({hr.rover_id})")

    # --- Settings: sent right after the handshake, and sane ----------------
    env = await app.receive("settings", timeout=1.0)
    r.check(env is not None, "settings arrive right after the handshake")
    st = env.settings
    r.check(0 < st.heartbeat_period_ms <= 1000 and 0 < st.state_period_ms <= 1000,
            f"heartbeat every {st.heartbeat_period_ms} ms, state every {st.state_period_ms} ms")
    r.check(st.route_pause_after_silence_min_ms <= st.route_pause_after_silence_ms
            <= st.route_pause_after_silence_max_ms,
            f"route pause {st.route_pause_after_silence_ms} ms is within its bounds "
            f"({st.route_pause_after_silence_min_ms}-{st.route_pause_after_silence_max_ms})")
    r.check(st.follow_stop_after_silence_ms > 0 and st.max_speed_mps > 0,
            f"follow stops after {st.follow_stop_after_silence_ms} ms, top speed "
            f"{st.max_speed_mps:.2f} m/s")

    # --- State: at the advertised rate, complete, increasing ---------------
    app.drain()
    window_s = 2.0
    states = await app.collect("state", window_s)
    expected = window_s * 1000 / st.state_period_ms
    r.check(0.7 * expected <= len(states) <= 1.3 * expected,
            f"state arrives at the advertised rate ({len(states) / window_s:.1f} Hz, "
            f"advertised {1000 / st.state_period_ms:.1f})")
    seqs = [e.state.seq for e in states]
    r.check(seqs == sorted(seqs) and len(set(seqs)) == len(seqs), "state sequence numbers only increase")
    r.check(all(e.rover_id == hr.rover_id for e in states), "every state carries the rover id")
    last = states[-1].state
    r.check(last.operating_state != pb.OPERATING_STATE_UNSPECIFIED,
            f"the state says what the rover is doing ({pb.OperatingState.Name(last.operating_state)})")
    for part in ("position", "heading"):
        if last.HasField(part):
            q = getattr(last, part).quality
            r.check(q != pb.QUALITY_UNSPECIFIED, f"{part} is sent with its own quality "
                    f"({pb.Quality.Name(q)})")

    if pb.CAPABILITY_READINESS in hr.capabilities:
        checks = last.readiness
        r.check(len(checks) > 0, f"readiness checks are sent ({len(checks)})")
        r.check(all(c.id != pb.CHECK_ID_UNSPECIFIED and c.status != pb.CHECK_STATUS_UNSPECIFIED
                    for c in checks), "every readiness check has an id and a status")
        r.check(len({c.id for c in checks}) == len(checks), "no readiness check appears twice")

    if pb.CAPABILITY_ALERTS in hr.capabilities:
        rank = {pb.SEVERITY_PROBLEM: 0, pb.SEVERITY_ACTION: 1, pb.SEVERITY_WARNING: 2,
                pb.SEVERITY_INFO: 3}
        sev = [rank.get(a.severity, 9) for a in last.active_alerts]
        r.check(sev == sorted(sev), f"active alerts are highest severity first ({len(sev)} active)")
        r.check(all(a.code != pb.ALERT_CODE_UNSPECIFIED and a.id for a in last.active_alerts),
                "every active alert has a code and an id")

    # --- Heartbeat: both sides measure the round trip on their own clock ----
    hbs = await app.collect("heartbeat", 1.0)
    r.check(len(hbs) >= 1000 // st.heartbeat_period_ms - 1,
            f"the rover sends heartbeats ({len(hbs)} in 1 s)")
    r.check(any(e.heartbeat.echo_seq for e in hbs), "the rover echoes our heartbeats")
    r.check(app.last_rtt_ms is not None and 0 <= app.last_rtt_ms < 1000,
            f"the app measures the round trip from the echo ({app.last_rtt_ms} ms)")
    link = (await app.receive("state")).state.link
    r.check(link.HasField("round_trip_ms") and link.round_trip_ms < 1000,
            f"the rover measures it too, from our echoes ({link.round_trip_ms} ms)")
    r.check(link.since_last_heard_ms < 2 * st.heartbeat_period_ms + 200,
            f"and knows the app is alive (last heard {link.since_last_heard_ms} ms ago)")

    # --- Rules 3 and 5: STOP, and safe to repeat ---------------------------
    app.drain()
    stop = stop_command()
    t0 = time.monotonic()
    await app.send(command=stop)
    res = await app.receive("command_result")
    took = (time.monotonic() - t0) * 1000
    r.check(res is not None and res.command_result.command_id == stop.command_id
            and res.command_result.outcome == pb.COMMAND_OUTCOME_ACCEPTED,
            f"STOP is accepted ({took:.0f} ms to the answer; target under 150 ms on the radio)")
    after = await app.receive("state")
    r.check(after.state.operating_state == pb.OPERATING_STATE_STOPPED and after.state.speed_mps == 0,
            "after STOP the state is STOPPED with zero speed")

    await app.send(command=stop)  # the same id again
    res2 = await app.receive("command_result")
    r.check(res2 is not None and res2.command_result == res.command_result,
            "resending the same STOP gets the same answer and has no second effect")

    # --- Wrong rover --------------------------------------------------------
    real_id, app.rover_id = app.rover_id, "some-other-rover"
    await app.send(command=stop_command())
    res3 = await app.receive("command_result")
    r.check(res3 is not None and res3.command_result.reject_reason == pb.REJECT_REASON_WRONG_ROVER,
            "a command addressed to another rover is rejected")
    app.rover_id = real_id


def is_local(url):
    host = urllib.parse.urlparse(url).hostname or ""
    return host in ("127.0.0.1", "localhost", "::1")


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--url", required=True, help="the rover's API, e.g. ws://127.0.0.1:8082/api")
    parser.add_argument("--allow-real-rover", action="store_true",
                        help="permit a non-local address; this test sends STOP")
    args = parser.parse_args()

    if not is_local(args.url) and not args.allow_real_rover:
        sys.exit(f"{args.url} is not on this PC. This test sends STOP. If the rover is parked and "
                 "someone has the RC transmitter, add --allow-real-rover.")

    r = Report()
    try:
        asyncio.run(contract(args.url, r))
    except Failed:
        sys.exit(1)
    print(f"\n{r.passed} contract checks passed against {args.url}")


if __name__ == "__main__":
    main()
