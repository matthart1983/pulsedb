# PulseLang Demo — APL-inspired query language for PulseDB
# Run: vhs demo.tape

Output demo.gif
Set Shell bash
Set FontSize 15
Set Width 900
Set Height 550
Set Padding 16
Set Theme "Catppuccin Mocha"
Set TypingSpeed 30ms
Set PlaybackSpeed 0.75

# Prepare environment (hidden)
Hide
Type "cd /Users/matt/projects/pulsedb && rm -rf /tmp/pulsedb_demo && mkdir -p /tmp/pulsedb_demo"
Enter
Sleep 1s
Type "export PATH=$PWD/target/release:$PATH"
Enter
Sleep 500ms
Type "clear"
Enter
Sleep 500ms
Show

# Launch the REPL
Type "pulsedb lang --data-dir /tmp/pulsedb_demo"
Sleep 300ms
Enter
Sleep 1.5s

# --- Scalar arithmetic ---
Type "2 * 3 + 4"
Sleep 200ms
Enter
Sleep 1s

Type "(2 * 3) + 4"
Sleep 200ms
Enter
Sleep 1s

# --- Vectors ---
Type "1 2 3 4 5 + 10"
Sleep 200ms
Enter
Sleep 1s

Type "sum 1 2 3 4 5"
Sleep 200ms
Enter
Sleep 1s

Type "sums 1 2 3 4 5"
Sleep 200ms
Enter
Sleep 1s

# --- Assignment + Aggregation ---
Type "vals: 10.0 20.0 30.0 40.0 50.0"
Sleep 200ms
Enter
Sleep 1s

Type "avg vals"
Sleep 200ms
Enter
Sleep 1s

Type "deltas vals"
Sleep 200ms
Enter
Sleep 1s

# --- Pipeline ---
Type "1 2 3 4 5 |> sum"
Sleep 200ms
Enter
Sleep 1s

# --- Lambda ---
Type "double: {x * 2}"
Sleep 200ms
Enter
Sleep 800ms

Type "double[21]"
Sleep 200ms
Enter
Sleep 1s

# --- Time-Series Primitives ---
Type "prices: 100.0 102.0 101.0 104.0 103.0 106.0"
Sleep 200ms
Enter
Sleep 800ms

Type "mavg[3; prices]"
Sleep 200ms
Enter
Sleep 1s

Type "ema[0.3; prices]"
Sleep 200ms
Enter
Sleep 1s

# --- Structural ops ---
Type "asc 3 1 4 1 5 9 2 6"
Sleep 200ms
Enter
Sleep 1s

Type "distinct 1 2 1 3 2 4 3"
Sleep 200ms
Enter
Sleep 1s

# --- Dict ---
Type "`a`b`c ! 10 20 30"
Sleep 200ms
Enter
Sleep 1s

# --- JSON output ---
Type "\fmt json"
Sleep 200ms
Enter
Sleep 800ms

Type "`name`score ! 42 99"
Sleep 200ms
Enter
Sleep 1.5s

Type "\fmt text"
Sleep 200ms
Enter
Sleep 600ms

# --- Conditional ---
Type "$[3 > 2; 1; 0]"
Sleep 200ms
Enter
Sleep 1s

# --- Exit PulseLang REPL ---
Type "exit"
Sleep 200ms
Enter
Sleep 1s

# ===== Python (Viper) Integration =====
Sleep 500ms

# Launch the Python REPL
Type "pulsedb python --data-dir /tmp/pulsedb_demo"
Sleep 300ms
Enter
Sleep 1.5s

# --- Insert data from Python ---
Type "db_insert('cpu', {'usage': 72.5, 'temp': 58.0}, {'host': 'web01'})"
Sleep 200ms
Enter
Sleep 1s

Type "db_insert('cpu', {'usage': 85.3, 'temp': 61.2}, {'host': 'web02'})"
Sleep 200ms
Enter
Sleep 1s

Type "db_insert('cpu', {'usage': 44.1, 'temp': 49.8}, {'host': 'web03'})"
Sleep 200ms
Enter
Sleep 1s

# --- Query back with PulseLang from Python ---
Type "result = db_query('avg cpu.usage')"
Sleep 200ms
Enter
Sleep 1s

Type "print(result)"
Sleep 200ms
Enter
Sleep 1s

# --- List measurements ---
Type "print(db_measurements())"
Sleep 200ms
Enter
Sleep 1s

# --- Use Python logic with DB data ---
Type@50ms "vals = db_query('cpu.usage')"
Sleep 200ms
Enter
Sleep 800ms

Type@50ms "for v in vals:"
Sleep 200ms
Enter
Type@50ms "    if v > 70.0:"
Sleep 200ms
Enter
Type@50ms "        print('ALERT: high CPU ' + str(v))"
Sleep 200ms
Enter
Enter
Sleep 1.5s

# --- Exit Python REPL ---
Sleep 500ms
Ctrl+D
Sleep 1s
