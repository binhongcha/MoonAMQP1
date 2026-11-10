"""Start an isolated Apache Artemis 2.44.0 broker and run the TCP interop test."""

import os
from pathlib import Path
import re
import socket
import subprocess
import sys
from tempfile import TemporaryDirectory
from time import monotonic, sleep


def run(artemis_home):
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin" / ("java.exe" if os.name == "nt" else "java")) if java_home else "java"
    boot = [
        java, "-Xmx256m", "-cp", str(artemis_home / "lib/artemis-boot.jar"),
        f"-Dartemis.home={artemis_home}",
    ]
    main = "org.apache.activemq.artemis.boot.Artemis"
    with TemporaryDirectory(prefix="moonamqp-artemis-") as directory:
        instance = Path(directory) / "broker"
        subprocess.run(
            boot + [main, "create", str(instance), "--silent", "--no-web", "--no-autotune",
                    "--allow-anonymous", "--user", "guest", "--password", "guest",
                    "--role", "guest", "--queues", "moon-out,moon-in"],
            check=True, timeout=40,
        )
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]
        etc = instance / "etc"
        config = etc / "broker.xml"
        xml, changes = re.subn(
            r"<acceptors>.*?</acceptors>",
            f'<acceptors><acceptor name="interop">tcp://127.0.0.1:{port}'
            '?protocols=AMQP;amqpIdleTimeout=0</acceptor></acceptors>',
            config.read_text(encoding="utf8"),
            count=1, flags=re.S,
        )
        if changes != 1:
            raise RuntimeError("Artemis acceptors could not be restricted to loopback")
        config.write_text(xml, encoding="utf8")
        log = Path(directory) / "artemis.log"
        launch = boot + [
            f"-Dartemis.instance={instance}", f"-Dartemis.instance.etc={etc}",
            f"-Ddata.dir={instance / 'data'}",
            f"-Djava.security.auth.login.config={etc / 'login.config'}",
            f"-Dlog4j2.configurationFile={(etc / 'log4j2.properties').as_uri()}",
            main, "run",
        ]
        with log.open("wb") as stream:
            broker = subprocess.Popen(launch, cwd=instance, stdout=stream, stderr=subprocess.STDOUT)
            try:
                deadline = monotonic() + 30
                while broker.poll() is None and monotonic() < deadline:
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                            break
                    except OSError:
                        sleep(0.25)
                else:
                    raise RuntimeError("Artemis did not open the test listener")
                subprocess.run(
                    ["node", str(Path(__file__).with_name("broker_interop.cjs"))],
                    env={**os.environ, "MOONAMQP1_BROKER_PORT": str(port)}, timeout=30, check=True,
                )
            except Exception:
                print(log.read_text(encoding="utf8", errors="replace"), file=sys.stderr)
                raise
            finally:
                broker.terminate()
                try:
                    broker.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    broker.kill()
                    broker.wait()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: python scripts/run_artemis_interop.py ARTEMIS_HOME")
    run(Path(sys.argv[1]).resolve(strict=True))
