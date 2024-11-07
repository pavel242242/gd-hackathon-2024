import streamlit as st
from gooddata_sdk import GoodDataSdk, CatalogDataSourceSnowflake, SnowflakeAttributes, BasicCredentials, CatalogGenerateLdmRequest,ExecutionDefinition,Attribute,SimpleMetric,ObjId, TableDimension
import requests
import json
from gooddata_pandas import GoodPandas


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
            if st.button("Save data source to PDM and gd_hackaton workspace", key = data_source.id):
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

# AI Visualization Generator
if st.checkbox("AI Visualization Generator"):
    st.subheader("Generate Visualization using AI")
    
    # Text input for the question
    user_question = st.text_input("Enter your visualization question:", 
                                 placeholder="E.g., create visualization how many jobs were run year by year?")
    
    if user_question and st.button("Generate Visualization"):
        try:
            url = f"{gd_host}/api/v1/actions/workspaces/gd_hackaton/ai/chat"
            
            payload = {
                "question": user_question,
                "deepSearch": True,
                "objectTypes": ["attribute", "fact"]
            }
            
            headers = {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'Authorization': f"Bearer {gd_token}"
            }
            
            response = requests.post(url, headers=headers, json=payload)
            
            if response.status_code == 200:
                st.success("Visualization generated successfully!")
                response_data = response.json()
                
                # Store the response in Streamlit's session state
                st.session_state["response_data"] = response_data
                
                # Display the response JSON
                st.json(response_data)
            else:
                st.error(f"Failed to generate visualization: {response.text}")
                
        except Exception as e:
            st.error(f"Error occurred: {e}")

# Additional code block that uses elements from the response JSON
if "response_data" in st.session_state:
    response_data = st.session_state["response_data"]
    
    # Extract the specific elements from the response
    try:
        gp = GoodPandas(gd_host, gd_token)
        frames = gp.data_frames("gd_hackaton")
        metric_id = response_data["createdVisualizations"]["objects"][0]["metrics"][0]["id"]
        agg_func = response_data["createdVisualizations"]["objects"][0]["metrics"][0]["aggFunction"]
        dimensionality_id = response_data["createdVisualizations"]["objects"][0]["dimensionality"][0]["id"]

    #   #  df = frames.for_items(
    #         items=dict(
    #             first_metric=f'metric/{metric_id}',
    #             first_dimension=f'label/{dimensionality_id}'
    #         )
    #     )
        exec_def = ExecutionDefinition(
            attributes=[
                Attribute(local_id=dimensionality_id, label=dimensionality_id)
            ],
            metrics=[
                SimpleMetric(local_id=metric_id, item=ObjId(id=metric_id, type="attribute"),aggregation=agg_func)
            ],
            filters=[],
            #dimensions=[[dimensionality_id], [ "measureGroup"]],
            dimensions=[TableDimension(item_ids=[dimensionality_id,"measureGroup"])],
        )
        df, df_metadata = frames.for_exec_def(exec_def=exec_def)
    
    # Display or further process `df` as needed
        st.write(df)

        st.session_state["metric_id"] = metric_id
        st.session_state["dimensionality_id"] = dimensionality_id

        st.write("Metric ID:", st.session_state["metric_id"])
        st.write("Dimensionality ID:", st.session_state["dimensionality_id"])

    except KeyError as e:
        st.error(f"KeyError: {e}")


