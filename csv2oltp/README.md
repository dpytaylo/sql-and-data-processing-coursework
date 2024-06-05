
# CSV to OLTP Data Loader

This project provides a script to load CSV data into an OLTP (Online Transaction Processing) system. Follow the instructions below to set up and run the script.

## Prerequisites

- Python 3.x
- Pip (Python package installer)
- Virtualenv

## Setup

1. **Configure Environment Variables**

   Set up the environment variables in the `.env` file. Possible values for the `csv_dir` variable are:

   - `./../1` - Path to the first dataset
   - `./../2` - Path to the second dataset

   Example `.env` file content:
   ```
   csv_dir=./../1
   ```

2. **Run the Script**

   Depending on your operating system, execute the appropriate script to set up the environment and run the data loader.

   ### For Windows:
   ```sh
   run.bat
   ```

   ### For Unix-based Systems:
   ```sh
   ./run.sh
   ```

3. **Manual Execution (if OS scripts don't work)**

   If the provided scripts do not work, you can set up and run the project manually with the following commands:

   ```sh
   python -m venv venv
   ./venv/Scripts/pip.exe install -r requirements.txt
   ./venv/Scripts/python.exe ./load_data.py
   ```

## Troubleshooting

- Ensure you have the correct paths in your `.env` file.
- Verify you have Python 3.x installed.
- Ensure all required Python packages are installed (`requirements.txt`).

For further assistance, please refer to the project's documentation or contact the support team.
