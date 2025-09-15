### Databricks CLI Installation and Configuration

#### Installation
1. Open the Command Prompt and run: `winget search databricks`
2. Install the Databricks CLI by executing: `winget install Databricks.DatabricksCLI`
3. Restart the Command Prompt and verify the installation with: `databricks -v`

#### Configuration
1. Navigate to User > Settings > Developer and generate a personal access token.
2. In the Command Prompt, run: `databricks configure`
3. Enter the Databricks workspace URL and your personal access token when prompted.
4. To verify the connection, run: `databricks users list` — this should display all users in the connected Databricks workspace.