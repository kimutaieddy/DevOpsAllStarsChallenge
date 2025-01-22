# NBA Data Lake

This repository includes the `data-lake.py` script, designed to automate the creation of a data lake for NBA analytics using AWS services. The script seamlessly integrates Amazon S3, AWS Glue, and Amazon Athena to set up the infrastructure required to store, process, and query NBA-related data.

---

## Overview

The `data-lake.py` script simplifies the creation of a data lake for NBA analytics using AWS services. This script leverages Amazon S3, AWS Glue, and Amazon Athena to establish the necessary infrastructure for storing, processing, and querying NBA-related data. Key functionalities include:

- Creating an Amazon S3 bucket for storing raw and processed data.
- Uploading NBA data (in JSON format) to the S3 bucket.
- Setting up an AWS Glue database and an external table for querying the data.
- Configuring Amazon Athena to enable data queries directly from the S3 bucket.

---

## Prerequisites

Before executing the script, ensure the following requirements are met:

1. **Python Installed**
   - Ensure Python is installed on your system. You can verify this by running:
     ```bash
     python --version
     ```

2. **SportsData.io Account**
   - Sign up for a free account at [SportsData.io](https://sportsdata.io).
   - Navigate to the **Developers** section and locate **API Resources**.
   - Opt for the **SportsDataIO API Free Trial**, provide the required information, and select NBA for this tutorial.
   - After receiving the confirmation email, access the **Developer Portal**, switch to the NBA section, and retrieve your API key from the **Standings** section under **Query String Parameters**.
   - Save the API key for later use in the script.

3. **AWS IAM Role/Permissions**
   - The user or role executing the script must have the following AWS permissions:
     - **S3:** `s3:CreateBucket`, `s3:PutObject`, `s3:DeleteBucket`, `s3:ListBucket`
     - **Glue:** `glue:CreateDatabase`, `glue:CreateTable`, `glue:DeleteDatabase`, `glue:DeleteTable`
     - **Athena:** `athena:StartQueryExecution`, `athena:GetQueryResults`

4. **AWS CLI Configuration**
   - Ensure your terminal is configured with AWS credentials for the desired user or role. You can do this by running:
     ```bash
     aws configure
     ```
   - Provide the appropriate access key, secret access key, and default region during configuration.

---

## Steps to Run the Project

1. **Clone the Repository**  
   Clone this repository and navigate to the project directory:  
   ```bash
   git clone https://github.com/yourusername/NBADataLake.git
   cd NBADataLake