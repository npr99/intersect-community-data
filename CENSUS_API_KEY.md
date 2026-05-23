# Setting Up Your Census API Key

As of **May 12, 2026**, the U.S. Census Bureau requires an API key on
**every** request to `api.census.gov`. Pyncoda fetches its core inputs
from that API, so without a key the workflow will stop with an error
that looks like:

```
The Census Data API requires an API key. Set the CENSUS_API_KEY
environment variable to a valid key. See CENSUS_API_KEY.md for free
signup and setup steps.
```

The key is **free**, takes about a minute to request, and arrives
instantly by email.

---

## Step 1 — Request a key

Go to <https://api.census.gov/data/key_signup.html>, fill in the short
form (organization name + email), and submit. The Census Bureau emails
you a 40-character hexadecimal key. Treat it like a password — don't
commit it to git or paste it into chat.

---

## Step 2 — Make the key available as an environment variable

Pyncoda reads the key from the environment variable `CENSUS_API_KEY`.
Pick the section below that matches your operating system.

### macOS and Linux

#### Option A — Set it for the current terminal only

This is the quickest way to try things out. The variable disappears
when you close the terminal.

```bash
export CENSUS_API_KEY=your-40-character-key-here
```

Verify it's set:

```bash
echo $CENSUS_API_KEY
```

#### Option B — Set it permanently (recommended for regular use)

Add the same `export` line to your shell's startup file so every new
terminal picks it up automatically.

- **macOS (default zsh):** edit `~/.zshrc`
- **Linux (bash):** edit `~/.bashrc` (or `~/.bash_profile` on some
  setups)
- **Either OS, fish shell:** run `set -Ux CENSUS_API_KEY your-key`
  instead — `fish` persists universal variables for you.

Example for zsh / bash:

```bash
echo 'export CENSUS_API_KEY=your-40-character-key-here' >> ~/.zshrc
source ~/.zshrc            # apply to the current terminal
```

Open a new terminal and confirm with `echo $CENSUS_API_KEY`.

---

### Windows

#### Option A — Command Prompt (current session only)

```cmd
set CENSUS_API_KEY=your-40-character-key-here
```

Verify:

```cmd
echo %CENSUS_API_KEY%
```

This lasts only until you close the Command Prompt window.

#### Option B — PowerShell (current session only)

```powershell
$env:CENSUS_API_KEY = "your-40-character-key-here"
```

Verify:

```powershell
echo $env:CENSUS_API_KEY
```

#### Option C — Set it permanently via the Settings UI (recommended)

1. Press **Windows key**, type **"environment variables"**, and select
   **"Edit the system environment variables"**.
2. In the dialog that opens, click the **Environment Variables…**
   button at the bottom.
3. Under **"User variables for <your username>"**, click **New…**.
4. Set **Variable name** to `CENSUS_API_KEY` and **Variable value** to
   your 40-character key. Click **OK** on each dialog.
5. **Close and reopen** any terminals, IDEs (VS Code, PyCharm), or
   Jupyter sessions so they pick up the new variable.

Verify in a fresh PowerShell window:

```powershell
echo $env:CENSUS_API_KEY
```

#### Option D — Set it permanently via PowerShell (no GUI)

```powershell
[System.Environment]::SetEnvironmentVariable(
    "CENSUS_API_KEY",
    "your-40-character-key-here",
    "User"
)
```

Close and reopen your terminal afterward.

---

## Step 3 — Make sure your IDE or Jupyter sees the variable

A common source of confusion: you set the variable in a terminal, but
your IDE or Jupyter notebook was already running and inherited the
**old** environment.

- **VS Code / PyCharm:** fully quit the application and reopen it
  after setting the variable.
- **Jupyter Notebook / Lab:** stop the kernel and the notebook
  server, then restart both from a terminal that has the variable
  set.
- **Quick sanity check inside Python:**

  ```python
  import os
  print(os.environ.get("CENSUS_API_KEY"))
  ```

  If this prints `None`, the variable isn't visible to your Python
  process yet — restart the IDE/Jupyter from a fresh terminal.

---

## Troubleshooting

- **"The Census Data API requires an API key"** — the variable is not
  set in the environment your Python process inherited. See Step 3.
- **"Census API rejected the CENSUS_API_KEY"** — the variable is set
  but the key itself is wrong (typo, extra whitespace, or revoked).
  Re-check the key in your activation email, or request a new one.
- **The key still doesn't work after a fresh terminal** — make sure
  you didn't wrap the value in quotes inside `.zshrc` / `.bashrc` if
  the key contains no special characters; `export CENSUS_API_KEY=abc…`
  is enough. Stray quotes become part of the value.

---

## Security note

- Don't commit your key to git. Add `CENSUS_API_KEY` (the file, if you
  ever save it locally) and any `.env` files to `.gitignore`.
- Don't paste the key into screenshots, issues, or chat logs. If you
  do, treat it as compromised and request a new one — old keys can be
  rotated by requesting a fresh signup with the same email.
