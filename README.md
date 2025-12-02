# SQL Server Database Tuner

Single-file T-SQL script that snapshots a SQL Server database's performance signals and schema into one Markdown report, designed to be read by humans and analysed by LLMs such as ChatGPT or Claude.

> This project is LLM-first: the Markdown output is optimised for tools like ChatGPT or Claude to read, navigate, and reason about.

## 1. What this is

Database Tuner is a **read-only diagnostic script** for Microsoft SQL Server.

- Collects a point-in-time view of server and database configuration, workload, storage, indexing, statistics, waits, and more.
- Stores results in temp tables in `tempdb`, then exports everything as one Markdown file (`dt_report (DatabaseName - Version).md`).
- The Markdown is structured so that an LLM can navigate it reliably: numbered sections, consistent layouts, and a built-in Assistant Brief that explains how to use the data.

It never changes your user data or schema. The only persistent write is the report file on the client machine.

## 2. Why another tool?

Community tools like Brent Ozar's First Responder Kit (for example `sp_Blitz`, `sp_BlitzCache`, `sp_BlitzIndex`) are excellent for rule-based health checks and triage.

Database Tuner takes a different, **Markdown + LLM-first** approach:

- Instead of immediately listing findings, it captures a broad diagnostic snapshot into a single, portable `.md` file.
- That Markdown report file can be versioned alongside your code, attached to tickets, or shared with an LLM for deeper analysis.
- The Assistant Brief and meta blocks are designed so that an LLM can:
  - Start from a Main Menu.
  - Propose Top-10 low-risk opportunities.
  - Drill into specific slices with clear "Source" and "Why" notes.

Think of this script as an **evidence pack generator**. A strong LLM can then read that evidence and explain trade-offs, rather than you or the tool hard-coding every rule.

## 3. Requirements

**SQL Server**

- Minimum engine version: **SQL Server 2017** (ProductMajorVersion 14).
- Target database compatibility level: **100 or higher** (SQL Server 2008+).
- Tested primarily on on-premises SQL Server (2017, 2019, 2022, 2025) and Azure SQL Managed Instance.
- **Not supported:** Azure SQL Database (single/elastic); several collectors depend on DMVs and features that are not available there.

**Permissions**

- Recommended: a login with **sysadmin** on the target instance.
- Many slices will still work with lower permissions (for example `VIEW SERVER STATE`, `VIEW DATABASE STATE`), but some DMV-heavy sections will return partial data or be gated.

**Client tools**

- SQL Server Management Studio (SSMS) with **SQLCMD Mode** enabled, or
- `sqlcmd` from the command line.

---

## 4. Files in this repository

- `database_tuner.sql` – main Database Tuner script.
- `LICENSE` – MIT license for the project.

---

## 5. Quick start (SSMS)

1. **Clone or download** this repository.
2. Open the Database Tuner script (for example `database_tuner.sql`) in **SSMS**.
3. Enable **SQLCMD Mode** (`Query` -> `SQLCMD Mode`).
4. In the `-- User Config` section near the top, set the variables, for example:

   ```sql
   :SETVAR TargetDB     "YourDatabaseName"
   :SETVAR OutputDir    "C:\Temp\DatabaseTuner\"      -- directory must already exist
   :SETVAR ExportSchema "1"                           -- 0 or 1 (1 as default)
   :SETVAR SafeMode     "1"                           -- 0 or 1 (1 as default)
   ```

5. Press **F5** to run the script.
6. When it completes, look in `OutputDir` for a file named like:

   ```text
   dt_report (YourDatabaseName - Version).md
   ```

7. Open the `.md` file in a Markdown viewer or upload it to your LLM of choice.

The script is read-only with respect to your databases. It creates temp tables, reads DMVs and catalog views, and uses SQLCMD `:OUT` to write the report.

---

## 6. Configuration (SQLCMD variables)

These variables are defined at the top of the Database Tuner script.

- **`TargetDB`**
  - Name of the database you want to profile.

- **`OutputDir`**
  - Directory on the **client** machine (SSMS or `sqlcmd` host) where the Markdown file is written.
  - The directory must already exist. If it does not, `sqlcmd` falls back to printing the Markdown to STDOUT (for example the SSMS Messages tab).

- **`ExportSchema`**
  - `0` = do not append a schema appendix.
  - `1` (default) = append a schema export to the end of the report. This includes definitions for views, procedures, functions, triggers, tables, constraints, indexes, and more.
  - This can be very verbose and may include sensitive code. Prefer `0` when sharing externally unless you explicitly need schema context.

- **`SafeMode`**
  - `0` = full detail.
  - `1` (default) = **Safe Mode ON**. Certain fields are redacted or gated and replaced with `[SafeMode]`. Examples include:
    - Server and instance names.
    - Host names and file system paths.
    - Some job commands, mail profiles, credentials, and SQL text.
  - Temp table schemas do not change; only values are masked. This makes it easier to share reports outside your organisation or with cloud-hosted LLMs.

These flags also appear in the report header and in the Assistant Brief so the LLM can see how the report was generated.

---

## 7. What is in the report

The generated Markdown is organised into numbered sections (slices) such as `00a. Metadata`, `04a. Index Usage`, `09w. Index Usage DMVs (raw)`, etc.

Each slice follows a consistent pattern:

- `## NNx. Title` heading.
- A short meta block in a ` ```text` fence with lines such as:
  - `Source: ...`
  - `Why: ...`
  - `Gate: ...` (version or permission requirements, Safe Mode notes)
  - `Notes: ...` (heuristics, scoring hints, or caveats)
- A ` ```csv` block containing the slice data, produced from a stable temp table schema.

At the top of the report there is an **Assistant Brief** that explains to the LLM:

- How to print an initial header block:
  - `## Database Tuner Report {Version}`
  - `Target DB: [{TargetDB}] on {Instance}`
  - `SQL Server Version: {SQL Server version/edition}`
  - `Database Compat: {compat level}`
  - `Export Schema: {On|Off}`
  - `Safe Mode: {On|Off}`
- How to use the Main Menu (1 to 7), helper entries (11 to 20), and Top-10 opportunities (T1 to T10).
- How to drill into per-area lists and deep dives.

The goal is to make the Markdown as **LLM-friendly** as possible while still being readable by humans.

---

## 8. Using the report with an LLM

A typical workflow looks like this:

1. Run the script and generate `dt_report (YourDatabaseName - Version).md`.
2. Upload the file to your LLM (for example ChatGPT, Claude, or another capable model).
3. Start with a prompt such as:

   > I have attached a Database Tuner report for a SQL Server database. Start at the Main Menu described in the Assistant Brief, summarise the overall health, then propose a ranked list of low-risk performance improvements with supporting evidence from the report.

4. Follow up with more targeted prompts, for example:
   - "Show me the most important index changes from the Top-10 list and explain why."
   - "Drill into parent 2 (T-SQL modules) and highlight risky patterns or easy wins."
   - "Look at Safe Mode notes and tell me which areas might be hiding sensitive values."

Because the script embeds guidance and consistent section names, the LLM can usually navigate the report without you having to memorise every slice.

---

## 9. Limitations and notes

- Focuses on **one database at a time**. Instance-wide context is included where relevant (for example waits, memory, volumes), but the main target is a single database.
- Some slices only return useful data when specific features are enabled, for example:
  - Query Store.
  - Availability Groups.
  - Accelerated Database Recovery (ADR).
  - Columnstore, In-Memory OLTP, or other advanced features.
- The script uses `SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED` where appropriate to avoid blocking, but it can still be relatively heavy on very large or busy systems. Consider running it during a quieter window first, and always test against a non-production or staging copy of the database before running it on a busy production system.
- `SafeMode` and `ExportSchema` reduce risk when sharing a report but cannot guarantee that no sensitive information remains. Always review the Markdown before sending it outside your organisation.
- Azure SQL Database (single/elastic) is out of scope. Azure SQL Managed Instance is supported but some slices may be partially populated depending on feature parity.

---

## 10. Project status

This script has been iterated on heavily and is used against real databases, but it should still be considered **experimental and evolving**.

- Expect slice layouts and scoring heuristics to improve over time.
- Always test in non-production first and review any recommendations from an LLM before applying them.

If you would like to contribute, you can:

- File issues with sample reports when a slice misbehaves or could be more useful.
- Suggest additional slices that would help your tuning workflow.
- Propose documentation or README improvements.

---

## 11. License

This project is licensed under the MIT License (see the `LICENSE` file).

In plain terms, the MIT License allows free personal and commercial use, modification, and redistribution of this script, as long as you keep the original copyright and license text with any copies or substantial portions of the software.
