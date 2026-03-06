# PulseDB Demo — Dual query languages: PulseLang + Python (Viper)
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

# ===== Part 1: PulseLang — APL-inspired query language =====

Type "pulsedb lang --data-dir /tmp/pulsedb_demo"
Sleep 300ms
Enter
Sleep 1.5s

# --- Vectors + arithmetic ---
Type "1 2 3 4 5 + 10"
Sleep 200ms
Enter
Sleep 1s

Type "sum 1 2 3 4 5"
Sleep 200ms
Enter
Sleep 1s

# --- Assignment + pipeline ---
Type "vals: 10.0 20.0 30.0 40.0 50.0"
Sleep 200ms
Enter
Sleep 800ms

Type "vals |> avg"
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

# --- Time-series: EMA ---
Type "prices: 100.0 102.0 101.0 104.0 103.0 106.0"
Sleep 200ms
Enter
Sleep 800ms

Type "ema[0.3; prices]"
Sleep 200ms
Enter
Sleep 1s

# --- Dict + JSON output ---
Type "`a`b`c ! 10 20 30"
Sleep 200ms
Enter
Sleep 1s

Type "\fmt json"
Sleep 200ms
Enter
Sleep 600ms

Type "`name`score ! 42 99"
Sleep 200ms
Enter
Sleep 1.5s

Type "\fmt text"
Sleep 200ms
Enter
Sleep 600ms

# --- Exit PulseLang ---
Type "exit"
Sleep 200ms
Enter
Sleep 1s

# ===== Part 2: Python (Viper) — scripting with DB access =====
Sleep 500ms

Type "pulsedb python --data-dir /tmp/pulsedb_demo"
Sleep 300ms
Enter
Sleep 1.5s

# --- Insert data ---
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

# --- Query back via PulseLang from Python ---
Type "result = db_query('avg cpu.usage')"
Sleep 200ms
Enter
Sleep 1s

Type "print(result)"
Sleep 200ms
Enter
Sleep 1s

# --- Introspection ---
Type "print(db_measurements())"
Sleep 200ms
Enter
Sleep 1s

Type "print(db_fields('cpu'))"
Sleep 200ms
Enter
Sleep 1s

# --- Python logic on DB data ---
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

# --- Exit ---
Sleep 500ms
Ctrl+D
Sleep 1s
