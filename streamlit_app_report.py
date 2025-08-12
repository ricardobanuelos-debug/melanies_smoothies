# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched   # 👈 agregado when_matched

st.title("🥤 Customize Your Smoothie! 🥤")
st.write("Orders that need to be filled.")

session = get_active_session()

# 1) Traer solo órdenes no llenadas
orders_df = (
    session.table("smoothies.public.orders")
    .filter(col("ORDER_FILLED") == False)
    .to_pandas()
)

if orders_df.empty:
    st.info("👍 There are no pending orders right now.")
    st.stop()

# 2) Editor
editable_df = st.data_editor(
    orders_df,
    use_container_width=True
)

# 3) Botón Submit
submitted = st.button("Submit")

if submitted:
    # Crear datasets para el merge
    og_dataset = session.table("smoothies.public.orders")
    edited_dataset = session.create_dataframe(editable_df)

    # MERGE para actualizar ORDER_FILLED
    og_dataset.merge(
        edited_dataset,
        (og_dataset['ORDER_UID'] == edited_dataset['ORDER_UID']),
        [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
    )

    st.success("Orders updated successfully", icon="👍")
