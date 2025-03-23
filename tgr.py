import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Set page config
st.set_page_config(
    page_title="TGR Dealer Analytics Dashboard",
    page_icon="🚗",
    layout="wide"
)


# Function to load data from Google Sheets
@st.cache_data(ttl=600)  # Cache data for 10 minutes
def load_data():
    try:
        # Set up Google Sheets API
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive'
        ]

        # Try using service account file first
        try:
            credentials = Credentials.from_service_account_file('sheet_access.json', scopes=scopes)
        except:
            # If file not found, try using secrets
            credentials_dict = st.secrets["gcp_service_account"]
            credentials = Credentials.from_service_account_info(credentials_dict, scopes=scopes)

        gc = gspread.authorize(credentials)

        # Open the spreadsheet
        spreadsheet = gc.open('TGR - EL ajans')

        # Get all sheets
        historical_sheet = spreadsheet.worksheet('historical')
        live_cars_sheet = spreadsheet.worksheet('live cars')
        dealer_seg_sheet = spreadsheet.worksheet('dealers segmentation')
        dealer_activity_sheet = spreadsheet.worksheet('dealers app activity')

        # Get data from historical sheet
        historical_headers = ['request_date', 'dealer_code', 'time_on_app', 'price', 'make', 'model',
                              'year', 'kilometers', 'sylndr_acquisition_price', 'market_retail_price',
                              'median_asked_price', 'refurbishment_cost', 'margin', 'dealer_name', 'dealer_phone']
        historical_data = historical_sheet.get_all_records(expected_headers=historical_headers)
        historical_df = pd.DataFrame(historical_data)

        # Convert numeric columns in historical_df
        numeric_columns = ['time_on_app', 'price', 'year', 'kilometers', 'sylndr_acquisition_price',
                           'market_retail_price', 'median_asked_price', 'refurbishment_cost', 'margin']
        for col in numeric_columns:
            historical_df[col] = pd.to_numeric(historical_df[col], errors='coerce')

        # Get data from live cars sheet
        live_cars_headers = ['date_key', 'sf_vehicle_name', 'make', 'model', 'year', 'kilometers']
        live_cars_data = live_cars_sheet.get_all_records(expected_headers=live_cars_headers)
        live_cars_df = pd.DataFrame(live_cars_data)

        # Convert numeric columns in live_cars_df
        live_cars_df['year'] = pd.to_numeric(live_cars_df['year'], errors='coerce')
        live_cars_df['kilometers'] = pd.to_numeric(live_cars_df['kilometers'], errors='coerce')

        # Get data from dealers segmentation sheet
        dealer_seg_headers = ['dealer_code', 'dealer_name', 'user_vs_dealer_flag',
                              'lifetime_avg_days_between_purchases', 'sixty_day_avg_days_between_purchases',
                              'lifetime_avg_days_between_requests', 'sixty_day_avg_days_between_requests',
                              'total_purchases_lifetime', 'total_purchases_60d', 'total_requests_lifetime',
                              'total_requests_60d', 'avg_purchases_per_month_lifetime',
                              'avg_purchases_per_month_60d', 'avg_requests_per_month_lifetime',
                              'avg_requests_per_month_60d', 'sold_cars_30d', 'buy_requests_30d',
                              'purchase_segment_lifetime', 'purchase_segment_60d', 'final_bucket_lifetime',
                              'final_bucket_60d', 'request_segment_lifetime', 'request_segment_60d',
                              'request_activity_bucket_lifetime', 'request_activity_bucket_60d']
        dealer_seg_data = dealer_seg_sheet.get_all_records(expected_headers=dealer_seg_headers)
        dealer_seg_df = pd.DataFrame(dealer_seg_data)

        # Convert numeric columns in dealer_seg_df
        numeric_seg_columns = [
            'lifetime_avg_days_between_purchases', 'sixty_day_avg_days_between_purchases',
            'lifetime_avg_days_between_requests', 'sixty_day_avg_days_between_requests',
            'total_purchases_lifetime', 'total_purchases_60d', 'total_requests_lifetime',
            'total_requests_60d', 'avg_purchases_per_month_lifetime', 'avg_purchases_per_month_60d',
            'avg_requests_per_month_lifetime', 'avg_requests_per_month_60d', 'sold_cars_30d', 'buy_requests_30d'
        ]
        for col in numeric_seg_columns:
            dealer_seg_df[col] = pd.to_numeric(dealer_seg_df[col], errors='coerce')

        # Get data from dealers app activity sheet
        activity_headers = ['dealer_code', 'dealer_name', 'active_days_30d',
                            'total_car_events_30d', 'active_days_7d', 'total_car_events_7d']
        activity_data = dealer_activity_sheet.get_all_records(expected_headers=activity_headers)
        activity_df = pd.DataFrame(activity_data)

        # Convert numeric columns in activity_df
        numeric_activity_columns = ['active_days_30d', 'total_car_events_30d', 'active_days_7d', 'total_car_events_7d']
        for col in numeric_activity_columns:
            activity_df[col] = pd.to_numeric(activity_df[col], errors='coerce')

        # Convert date columns
        historical_df['request_date'] = pd.to_datetime(historical_df['request_date'])
        live_cars_df['date_key'] = pd.to_datetime(live_cars_df['date_key'])

        return historical_df, live_cars_df, dealer_seg_df, activity_df

    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


def main():
    st.title("🚗 Account Manager Dashboard - R")

    # Load data
    with st.spinner("Loading data..."):
        historical_df, live_cars_df, dealer_seg_df, activity_df = load_data()

    if historical_df.empty or dealer_seg_df.empty:
        st.warning("No data available. Please check your Google Sheet connection.")
        return

    # Sidebar filters
    st.sidebar.header("Filters")

    # Get list of dealers that exist in both segmentation and activity data
    valid_dealers = sorted(set(dealer_seg_df['dealer_name'].unique()) & set(activity_df['dealer_name'].unique()))

    if not valid_dealers:
        st.error("No dealers found with both segmentation and activity data.")
        return

    # Dealer selection
    selected_dealer = st.sidebar.selectbox(
        "Select Dealer",
        options=valid_dealers,
        index=0
    )

    # Get dealer details
    dealer_info = dealer_seg_df[dealer_seg_df['dealer_name'] == selected_dealer]
    dealer_activity = activity_df[activity_df['dealer_name'] == selected_dealer]

    if dealer_info.empty:
        st.error(f"No segmentation data found for dealer: {selected_dealer}")
        return

    if dealer_activity.empty:
        st.error(f"No activity data found for dealer: {selected_dealer}")
        return

    dealer_info = dealer_info.iloc[0]
    dealer_activity = dealer_activity.iloc[0]

    # Main content
    st.header(f"Dealer Profile: {selected_dealer}")

    # Key metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Total Lifetime Purchases", dealer_info['total_purchases_lifetime'])
        st.metric("Last 60 Days Purchases", dealer_info['total_purchases_60d'])

    with col2:
        st.metric("Total Lifetime Requests", dealer_info['total_requests_lifetime'])
        st.metric("Last 60 Days Requests", dealer_info['total_requests_60d'])

    with col3:
        st.metric("Active Days (30d)", dealer_activity['active_days_30d'])
        st.metric("Car Events (30d)", dealer_activity['total_car_events_30d'])

    with col4:
        st.metric("Active Days (7d)", dealer_activity['active_days_7d'])
        st.metric("Car Events (7d)", dealer_activity['total_car_events_7d'])

    # Segmentation Information
    st.subheader("Dealer Segmentation")

    seg_col1, seg_col2 = st.columns(2)

    with seg_col1:
        st.info(f"Lifetime Segment: {dealer_info['final_bucket_lifetime']}")
        st.info(f"60-Day Segment: {dealer_info['final_bucket_60d']}")

    with seg_col2:
        st.info(f"Request Activity (Lifetime): {dealer_info['request_activity_bucket_lifetime']}")
        st.info(f"Request Activity (60d): {dealer_info['request_activity_bucket_60d']}")

    # Historical Purchase Analysis
    st.subheader("Historical Purchase Analysis")
    dealer_historical = historical_df[historical_df['dealer_name'] == selected_dealer].copy()

    if not dealer_historical.empty:
        # Clean the data for visualization
        dealer_historical['kilometers'] = dealer_historical['kilometers'].fillna(dealer_historical['kilometers'].mean())

        # Create tabs for different visualizations
        hist_tab1, hist_tab2, hist_tab3 = st.tabs(["Purchase Timeline", "Price Distribution", "Make Distribution"])

        with hist_tab1:
            # Create time series of purchases with better handling of missing values
            fig = px.scatter(dealer_historical,
                             x='request_date',
                             y='price',
                             color='make',
                             size='kilometers',
                             hover_data=['model', 'year', 'margin'],
                             title='Historical Purchases Over Time',
                             size_max=30)  # Limit maximum bubble size

            # Customize the layout
            fig.update_layout(
                xaxis_title="Request Date",
                yaxis_title="Price (EGP)",
                showlegend=True
            )
            st.plotly_chart(fig, use_container_width=True)

        with hist_tab2:
            # Price distribution by make
            fig = px.box(dealer_historical,
                         x='make',
                         y='price',
                         title='Price Distribution by Make',
                         points="all")  # Show all points
            st.plotly_chart(fig, use_container_width=True)

        with hist_tab3:
            # Make distribution
            make_counts = dealer_historical['make'].value_counts()
            fig = px.pie(values=make_counts.values,
                         names=make_counts.index,
                         title='Distribution of Makes in Historical Purchases')
            st.plotly_chart(fig, use_container_width=True)

        # Show recent purchases with better formatting
        st.subheader("Recent Purchase History")
        recent_purchases = dealer_historical.sort_values('request_date', ascending=False).head(10)

        # Format the dataframe for display
        display_df = recent_purchases[['request_date', 'make', 'model', 'year', 'kilometers', 'price', 'margin']].copy()

        # Format numeric columns
        display_df['price'] = display_df['price'].apply(lambda x: f"{x:,.0f} EGP" if pd.notnull(x) else "N/A")
        display_df['kilometers'] = display_df['kilometers'].apply(lambda x: f"{x:,.0f} km" if pd.notnull(x) else "N/A")
        display_df['margin'] = display_df['margin'].apply(lambda x: f"{x:,.0f} EGP" if pd.notnull(x) else "N/A")

        # Format date column
        display_df['request_date'] = display_df['request_date'].dt.strftime('%Y-%m-%d')

        st.dataframe(
            display_df,
            column_config={
                "request_date": "Request Date",
                "make": "Make",
                "model": "Model",
                "year": "Year",
                "kilometers": "Mileage",
                "price": "Price",
                "margin": "Margin"
            },
            use_container_width=True
        )

        # Add summary statistics
        st.subheader("Purchase Summary Statistics")
        col1, col2, col3 = st.columns(3)

        with col1:
            avg_price = dealer_historical['price'].mean()
            st.metric("Average Purchase Price", f"{avg_price:,.0f} EGP")

        with col2:
            avg_km = dealer_historical['kilometers'].mean()
            st.metric("Average Mileage", f"{avg_km:,.0f} km")

        with col3:
            avg_margin = dealer_historical['margin'].mean()
            st.metric("Average Margin", f"{avg_margin:,.0f} EGP")

    else:
        st.warning("No historical purchase data available for this dealer")

    # Recommended Cars
    st.subheader("Recommended Cars")
    # Here you would implement your car recommendation logic based on historical purchases
    # For now, we'll just show available cars that match their historical preferences

    if not dealer_historical.empty:
        preferred_makes = dealer_historical['make'].value_counts().index[:3].tolist()
        recommended_cars = live_cars_df[live_cars_df['make'].isin(preferred_makes)]

        if not recommended_cars.empty:
            st.dataframe(
                recommended_cars,
                use_container_width=True
            )
        else:
            st.info("No current cars match this dealer's preferences")
    else:
        st.info("Cannot generate recommendations without historical data")


if __name__ == "__main__":
    main()
