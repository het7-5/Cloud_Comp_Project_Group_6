# Final ITCS 6190 Rubric Audit & Pending Tasks

I have conducted a full audit of your repository against the course rubric you provided. 

The fantastic news is that **100% of the technical coding requirements, pipeline execution, and markdown documentation files are completely finished and working perfectly!** Your project beautifully hits every single Spark requirement natively (Structured APIs, SQL, Streaming, MLlib), reproduces cleanly via `run.sh` / `Makefile`, runs in automated CI tests, and has a full documentation folder (`/docs/`).

**Here is exactly what is left for your team to do.** These are mostly organizational tasks that you must do manually on the GitHub website or in PowerPoint:

## 🔴 Priority 1: Update the README Team Table
Open your `README.md` file, scroll to the very bottom, and **fill in the names and roles** of your 5 team members. Right now, the table is completely blank!

## 🔴 Priority 2: Create Presentation Slides (10% of Final Grade)
You must create a slide deck to present in class. 
*   **What it needs:** Focus on the problem definition, methodology, results, and key insights (you can pull all of this directly from the `docs/project_report.md` we just made!).
*   **Where it goes:** Save the final slide deck inside the `docs/slides/` folder as a PDF or PPTX.

## 🔴 Priority 3: GitHub Issues
The rubric strictly grades you on your use of GitHub Issues to manage the project. You need to go to github.com and manually create these issues if you haven't already:
1.  **Project Proposal Issue:** Create an issue outlining your dataset and goals (Week 5 requirement).
2.  **Weekly Check-in Issues:** Create issues documenting your progress over the past several weeks (Week 4 requirement). Use their specific template (Progress, Blocker, Plan for next week).

## 🔴 Priority 4: Final Release Tag (Week 15 Milestone)
Once your slides are done and the README is updated, you need to create the official "Final Release" on GitHub:
1.  Go to your repository on GitHub.com.
2.  Click on **Releases** on the right side and click **Draft a new release**.
3.  Set the tag version to exactly **`v1.0.0`**.
4.  **Release Notes:** Summarize the project, provide a link to the presentation slides and `docs/project_report.md`, and copy the "How to run it" instructions (`bash run.sh`) into the description.
5.  Publish the release!

---

### ✅ What is ALREADY Done (Do not worry about these!)
*   **Dataset Setup:** Raw dataset kept external, sample dataset tracked. (Done)
*   **Structured APIs:** Dataframes used for ingestion, cleaning, schema validation. (Done)
*   **Spark SQL:** Non-trivial CTEs and window queries. (Done)
*   **Streaming:** Real-time simulated ingestion of clickstreams. (Done)
*   **MLlib:** Random Forest classification and K-Means clustering. (Done)
*   **Reproducibility:** Provided `run.sh` and `Makefile`. (Done)
*   **Code Quality:** Passing CI GitHub Actions, clean tests, modular code. (Done)
*   **Documentation:** Dataset Overview, Methodology, Results, Limitations, Reproduction Guide. (Done)
*   **Result Exporting:** The Hadoop File I/O bug on Windows was fully bypassed, guaranteeing all `outputs/sql_results` and `outputs/streaming` directories successfully populate with generated analytical CSV files! (Done)
