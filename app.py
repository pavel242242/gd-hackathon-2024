import streamlit as st
from gooddata_sdk import GoodDataSdk, CatalogDataSourceSnowflake, SnowflakeAttributes, BasicCredentials, CatalogGenerateLdmRequest


# Initialize GoodData SDK using API token and host from secrets
gd_host = st.secrets["GD_Host"]
gd_token = st.secrets["GD_API_Token"]
sdk = GoodDataSdk.create(gd_host, gd_token)

# Streamlit UI
st.header("Browse GoodData Content")

# Fetch workspaces only once to improve performance
workspaces = []
try:
    with st.spinner("Fetching workspaces..."):
        workspaces = sdk.catalog_workspace.list_workspaces()
except Exception as e:
    st.error(f"Error fetching workspaces: {e}")

# Browse GoodData Workspaces
with st.expander("Show Workspaces"):
    st.subheader("GoodData Workspaces")
    for workspace in workspaces:
        st.write(f"- ID: {workspace.id}, Name: {workspace.name}")

# Browse GoodData Data Sources
if st.checkbox("Show Data Sources"):
    st.subheader("GoodData Data Sources")
    try:
        data_sources = sdk.catalog_data_source.list_data_sources()
        for data_source in data_sources:
            st.write(f"- ID: {data_source.id}, Name: {data_source.name}")
            if st.button("Save PDM", key = data_source.id):
                x = sdk.catalog_data_source.scan_data_source(data_source_id=data_source.id)
                #st.write(x)
                logical_model = sdk.catalog_data_source.generate_logical_model(data_source_id=data_source.id,generate_ldm_request= CatalogGenerateLdmRequest(pdm=x.pdm,grain_prefix='gr', reference_prefix='r', fact_prefix='f'))
                st.write('logical_model:')
                st.write("Succesfully read")
                try:
                    st.write("Trying to put ldm to workspace")
                    sdk.catalog_workspace_content.put_declarative_ldm(workspace_id="gd_hackaton", ldm=logical_model)
                    st.write("Succesfully written")
                except Exception as e:
                    st.error(f"Failed to create data source: {e}")
    except Exception as e:
        st.error(f"Error fetching data sources: {e}")
# Option to add a new Snowflake data source
with st.expander("Add Snowflake Data Source"):
    st.subheader("Configure New Snowflake Data Source")

    # Collect user input for Snowflake connection
    data_source_id = st.text_input("Data Source ID", "chocho")
    data_source_name = st.text_input("Data Source Name", "chocho")
    snowflake_account = st.text_input("Snowflake Account", "chocho")
    snowflake_warehouse = st.text_input("Snowflake Warehouse", "chocho")
    snowflake_dbname = st.text_input("Snowflake Database Name", "chocho")
    snowflake_schema = st.text_input("Snowflake Schema", "chocho")
    snowflake_user = st.text_input("Snowflake Username", "chocho")
    snowflake_password = st.text_input("Snowflake Password", "chocho", type='password')

    if st.button("Create Data Source"):
        try:
            sdk.catalog_data_source.create_or_update_data_source(
                CatalogDataSourceSnowflake(
                    id=data_source_id,
                    name=data_source_name,
                    db_specific_attributes=SnowflakeAttributes(
                        account=snowflake_account,
                        warehouse=snowflake_warehouse,
                        db_name=snowflake_dbname,
                    ),
                    schema=snowflake_schema,
                    credentials=BasicCredentials(
                        username=snowflake_user,
                        password=snowflake_password,
                    ),
                )
            )
            st.success("Data source created successfully!")
        except Exception as e:
            st.error(f"Failed to create data source: {e}")
            st.write(CatalogDataSourceSnowflake(
                    id=data_source_id,
                    name=data_source_name,
                    db_specific_attributes=SnowflakeAttributes(
                        account=snowflake_account,
                        warehouse=snowflake_warehouse,
                        db_name=snowflake_dbname,
                    ),
                    schema=snowflake_schema,
                    credentials=BasicCredentials(
                        username=snowflake_user,
                        password=snowflake_password,
                    ),
                )
            )

# Browse GoodData Logical Data Model (Datasets, Attributes, Facts) for a workspace
if st.checkbox("Show Data Models"):
    selected_workspace_id = st.selectbox(
        "Select Workspace for Data Model", options=[ws.id for ws in workspaces]
    )
    if selected_workspace_id:
        st.subheader(f"Data Model for Workspace: {selected_workspace_id}")
        try:
            ldm = sdk.catalog_workspace_content.get_declarative_ldm(selected_workspace_id)
            datasets = ldm.ldm.datasets

            for dataset in datasets:
                st.write(f"Dataset ID: {dataset.id}, Title: {dataset.title}")
                st.write("  Attributes:")
                for attribute in dataset.attributes:
                    st.write(f"    - {attribute.id} ({attribute.title})")
                st.write("  Facts:")
                for fact in dataset.facts:
                    st.write(f"    - {fact.id} ({fact.title})")
        except Exception as e:
            st.error(f"Error fetching data model: {e}")

# Browse GoodData Visualizations
if st.checkbox("Show Visualizations"):
    selected_workspace_for_viz = st.selectbox(
        "Select Workspace for Visualizations", options=[ws.id for ws in workspaces]
    )
    if selected_workspace_for_viz:
        st.subheader(f"Visualizations in Workspace: {selected_workspace_for_viz}")
        try:
            visualizations = sdk.insights.list_insights(selected_workspace_for_viz)
            for viz in visualizations:
                st.write(f"- ID: {viz.id}, Title: {viz.title}")
        except Exception as e:
            st.error(f"Error fetching visualizations: {e}")
