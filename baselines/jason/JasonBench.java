/**
 * baselines/jason/JasonBench.java
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * Entry point for the Jason BDI baseline benchmark.
 *
 * Generates a temporary .mas2j project file with the requested cycles,
 * warmup, and output path baked into the environment init args, then hands
 * off to jason.infra.centralised.RunCentralisedMAS. The custom RobotArmEnv
 * writes JSONL per-cycle latencies to the output file and calls
 * System.exit(0) once it has recorded `warmup + cycles` actions.
 *
 * Usage:
 *   java -cp <jason-jars>:. JasonBench --out latencies.jsonl --cycles 10000 --warmup 1000
 *
 * The Python harness (baselines/jason/run_jason.py) wraps this.
 */

import jason.infra.local.RunLocalMAS;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class JasonBench {

    public static void main(String[] args) throws Exception {
        String out = "jason_latencies.jsonl";
        int cycles = 10_000;
        int warmup = 1_000;
        Integer inject = null;
        String aslDir = "baselines/jason";
        String aslFile = "robot_arm.asl";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--out":     out = args[++i]; break;
                case "--cycles":  cycles = Integer.parseInt(args[++i]); break;
                case "--warmup":  warmup = Integer.parseInt(args[++i]); break;
                case "--inject":  inject = Integer.parseInt(args[++i]); break;
                case "--asl-dir": aslDir = args[++i]; break;
                case "--asl":     aslFile = args[++i]; break;
                default:
                    System.err.println("Unknown arg: " + args[i]);
                    System.exit(64);
            }
        }

        // Wrap each arg in quotes so the mas2j parser sees string literals
        // rather than bare atoms with '=' operators (which it rejects).
        StringBuilder envArgs = new StringBuilder();
        envArgs.append("\"out=").append(escape(out)).append("\"")
               .append(", \"cycles=").append(cycles).append("\"")
               .append(", \"warmup=").append(warmup).append("\"");
        if (inject != null) envArgs.append(", \"inject=").append(inject).append("\"");

        String mas2j =
            "MAS robot_arm {\n"
          + "    infrastructure: Local\n"
          + "    environment: RobotArmEnv(" + envArgs + ")\n"
          + "    agents:\n"
          + "        robot " + aslFile + ";\n"
          + "    aslSourcePath:\n"
          + "        \"" + aslDir.replace("\\", "/") + "\";\n"
          + "}\n";

        Path tmp = Files.createTempFile("robot_arm_bench_", ".mas2j");
        try (PrintWriter pw = new PrintWriter(tmp.toFile())) {
            pw.print(mas2j);
        }

        // Hand off to Jason. RobotArmEnv calls System.exit(0) when done.
        RunLocalMAS.main(new String[]{tmp.toAbsolutePath().toString()});
    }

    private static String escape(String s) {
        // mas2j init-arg parser is whitespace-sensitive; strip path separators
        // that confuse it on Windows by switching to forward slashes.
        return s.replace("\\", "/");
    }

    /** Suppress unused warning. */
    @SuppressWarnings("unused")
    private static Path resolve(String p) { return Paths.get(p); }
}
