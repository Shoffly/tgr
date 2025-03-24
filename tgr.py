import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Set page config
st.set_page_config(
    page_title="TR - Sales Analytics Tool",
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


def get_recommended_cars(dealer_historical, live_cars_df):
    if dealer_historical.empty:
        return pd.DataFrame()

    # Calculate dealer preferences
    make_preferences = dealer_historical['make'].value_counts()
    make_score_weights = make_preferences / make_preferences.max() * 3  # Max 3 points for make

    # Calculate model preferences within each make
    model_preferences = dealer_historical.groupby(['make', 'model']).size().reset_index(name='count')
    model_preferences['score'] = model_preferences.groupby('make')['count'].transform(
        lambda x: (x / x.max() * 2) if x.max() > 0 else 0  # Max 2 points for model
    )
    model_score_dict = dict(zip(
        zip(model_preferences['make'], model_preferences['model']),
        model_preferences['score']
    ))

    # Calculate year preferences
    avg_year = dealer_historical['year'].mean()
    std_year = dealer_historical['year'].std()

    def score_year(year):
        if pd.isna(year) or pd.isna(avg_year):
            return 0
        diff = abs(year - avg_year)
        if diff <= std_year:
            return 2.0  # Perfect match
        elif diff <= std_year * 2:
            return 1.0  # Good match
        elif diff <= std_year * 3:
            return 0.5  # Acceptable match
        return 0.0  # Poor match

    # Calculate kilometer preferences
    avg_km = dealer_historical['kilometers'].mean()
    std_km = dealer_historical['kilometers'].std()

    def score_kilometers(km):
        if pd.isna(km) or pd.isna(avg_km):
            return 0
        diff = abs(km - avg_km)
        if diff <= std_km:
            return 2.0  # Perfect match
        elif diff <= std_km * 2:
            return 1.0  # Good match
        elif diff <= std_km * 3:
            return 0.5  # Acceptable match
        return 0.0  # Poor match

    # Score each car in live inventory
    recommended_cars = live_cars_df.copy()

    # Score based on make preference (0-3 points)
    recommended_cars['make_score'] = recommended_cars['make'].map(make_score_weights).fillna(0)

    # Score based on model preference (0-2 points)
    recommended_cars['model_score'] = recommended_cars.apply(
        lambda x: model_score_dict.get((x['make'], x['model']), 0),
        axis=1
    )

    # Score based on year (0-2 points)
    recommended_cars['year_score'] = recommended_cars['year'].apply(score_year)

    # Score based on kilometers (0-2 points)
    recommended_cars['km_score'] = recommended_cars['kilometers'].apply(score_kilometers)

    # Calculate total score (max 9 points)
    recommended_cars['match_score'] = (
            recommended_cars['make_score'] +
            recommended_cars['model_score'] +
            recommended_cars['year_score'] +
            recommended_cars['km_score']
    )

    # Add score breakdown for transparency
    recommended_cars['score_breakdown'] = recommended_cars.apply(
        lambda x: f"Make: {x['make_score']:.1f}, Model: {x['model_score']:.1f}, "
                  f"Year: {x['year_score']:.1f}, KM: {x['km_score']:.1f}",
        axis=1
    )

    # Sort by score and return top matches
    return (recommended_cars
            .sort_values('match_score', ascending=False)
            .drop(['make_score', 'model_score', 'year_score', 'km_score'], axis=1)
            .head(10))


def get_dealers_needing_attention(dealer_seg_df):
    """Identify dealers who have moved from better to worse buckets."""

    # Define bucket hierarchy (from best to worst)
    bucket_hierarchy = {
        'Frequent': 5,
        'Active': 4,
        'Churned': 3,
        '1 Time Purchaser': 2,
        'No Purchase': 1
    }

    # Add bucket score columns for comparison
    attention_needed = dealer_seg_df.copy()
    attention_needed['lifetime_score'] = attention_needed['final_bucket_lifetime'].map(bucket_hierarchy)
    attention_needed['current_score'] = attention_needed['final_bucket_60d'].map(bucket_hierarchy)
    attention_needed['bucket_drop'] = attention_needed['lifetime_score'] - attention_needed['current_score']

    # Filter dealers who have dropped in bucket status
    attention_needed = attention_needed[attention_needed['bucket_drop'] > 0].copy()

    # Assign priority based on severity of drop and original status
    def get_priority(row):
        if row['final_bucket_lifetime'] == 'Frequent':
            if row['final_bucket_60d'] == 'No Purchase':
                return '🔴 Critical Priority'
            elif row['final_bucket_60d'] in ['Churned', '1 Time Purchaser']:
                return '🟠 High Priority'
            else:  # Active
                return '🟡 Medium Priority'
        elif row['final_bucket_lifetime'] == 'Active':
            if row['final_bucket_60d'] == 'No Purchase':
                return '🟠 High Priority'
            else:  # Churned or 1 Time Purchaser
                return '🟡 Medium Priority'
        else:
            return '⚪ Low Priority'

    attention_needed['status'] = attention_needed.apply(get_priority, axis=1)

    # Calculate activity metrics
    attention_needed['activity_drop'] = (
            attention_needed['avg_requests_per_month_lifetime'] -
            attention_needed['avg_requests_per_month_60d']
    )

    # Add transition description
    attention_needed['transition'] = attention_needed.apply(
        lambda x: f"{x['final_bucket_lifetime']} → {x['final_bucket_60d']}",
        axis=1
    )

    # Sort by priority (Critical -> High -> Medium -> Low) and then by bucket drop
    priority_order = ['🔴 Critical Priority', '🟠 High Priority', '🟡 Medium Priority', '⚪ Low Priority']
    return attention_needed.sort_values(
        ['status', 'bucket_drop', 'activity_drop'],
        ascending=[True, False, False],
        key=lambda x: pd.Categorical(x, categories=priority_order) if x.name == 'status' else x
    )


def main():
    st.title("🚗 TR - Sales Analytics Tool")

    # Load data
    with st.spinner("Loading data..."):
        historical_df, live_cars_df, dealer_seg_df, activity_df = load_data()

    if historical_df.empty or dealer_seg_df.empty:
        st.warning("No data available. Please check your Google Sheet connection.")
        return

    # Create main navigation
    main_tab1, main_tab2 = st.tabs(["📥 Attention Inbox", "👤 Dealer Profile"])

    with main_tab1:
        # Get dealers needing attention
        attention_dealers = get_dealers_needing_attention(dealer_seg_df)

        if attention_dealers.empty:
            st.success("No dealers currently need attention! 🎉")
        else:
            # Display summary metrics
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Cases", len(attention_dealers))
            with col2:
                critical_priority = len(attention_dealers[attention_dealers['status'] == '🔴 Critical Priority'])
                st.metric("Critical Priority", critical_priority)
            with col3:
                high_priority = len(attention_dealers[attention_dealers['status'] == '🟠 High Priority'])
                st.metric("High Priority", high_priority)
            with col4:
                med_priority = len(attention_dealers[attention_dealers['status'] == '🟡 Medium Priority'])
                st.metric("Medium Priority", med_priority)

            # Display dealer list with key metrics and direct links
            for _, dealer in attention_dealers.iterrows():
                with st.expander(
                        f"{dealer['status']} - {dealer['dealer_name']} ({dealer['transition']})"
                ):
                    col1, col2 = st.columns([2, 1])

                    with col1:
                        st.write("**Activity Metrics:**")
                        st.write(f"• Lifetime Monthly Requests: {dealer['avg_requests_per_month_lifetime']:.1f}")
                        st.write(f"• Recent Monthly Requests: {dealer['avg_requests_per_month_60d']:.1f}")
                        st.write(f"• Activity Drop: {dealer['activity_drop']:.1f} requests/month")

                    with col2:
                        st.write("**Recent Activity:**")
                        st.write(f"• 30-day Requests: {dealer['buy_requests_30d']}")
                        st.write(f"• 30-day Purchases: {dealer['sold_cars_30d']}")

                    # Add navigation button
                    if st.button("👤 View Full Profile", key=f"view_profile_{dealer['dealer_name']}"):
                        st.query_params["dealer"] = dealer['dealer_name']
                        st.query_params["tab"] = "Dealer Profile"
                        st.rerun()

    with main_tab2:
        # Sidebar filters
        st.sidebar.header("Filters")

        # Get list of dealers that exist in both segmentation and activity data
        valid_dealers = sorted(set(dealer_seg_df['dealer_name'].unique()) & set(activity_df['dealer_name'].unique()))

        if not valid_dealers:
            st.error("No dealers found with both segmentation and activity data.")
            return

        # Dealer selection - check if we came from inbox
        default_dealer = st.query_params.get('dealer', None)
        default_index = valid_dealers.index(default_dealer) if default_dealer in valid_dealers else 0

        # Dealer selection
        selected_dealer = st.sidebar.selectbox(
            "Select Dealer",
            options=valid_dealers,
            index=default_index
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

        # Move Recommended Cars section here
        st.subheader("Recommended Cars")
        dealer_historical = historical_df[historical_df['dealer_name'] == selected_dealer].copy()

        if not dealer_historical.empty:
            recommended_cars = get_recommended_cars(dealer_historical, live_cars_df)

            if not recommended_cars.empty:
                st.dataframe(
                    recommended_cars,
                    column_config={
                        "match_score": st.column_config.ProgressColumn(
                            "Match Score",
                            help="How well this car matches the dealer's preferences (max 9 points)",
                            format="%.1f",
                            min_value=0,
                            max_value=9
                        ),
                        "score_breakdown": "Score Breakdown",
                        "sf_vehicle_name": "Vehicle",
                        "make": "Make",
                        "model": "Model",
                        "year": "Year",
                        "kilometers": st.column_config.NumberColumn(
                            "Mileage",
                            format="%d km"
                        )
                    },
                    use_container_width=True
                )

                # Add explanation of scoring system
                st.info("""
                **Scoring System:**
                - Make: 0-3 points (based on frequency of purchases)
                - Model: 0-2 points (based on frequency within make)
                - Year: 0-2 points (based on preferred year ranges)
                - Mileage: 0-2 points (based on preferred km ranges)
                Total possible score: 9 points
                """)
            else:
                st.info("No matching cars found in current inventory")
        else:
            st.info("Cannot generate recommendations without historical data")

        # Historical Purchase Analysis (now after Recommended Cars)
        st.subheader("Historical Purchase Analysis")

        if not dealer_historical.empty:
            # Clean the data for visualization
            dealer_historical['kilometers'] = dealer_historical['kilometers'].fillna(
                dealer_historical['kilometers'].mean())

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
            display_df = recent_purchases[
                ['request_date', 'make', 'model', 'year', 'kilometers', 'price', 'margin']].copy()

            # Format numeric columns
            display_df['price'] = display_df['price'].apply(lambda x: f"{x:,.0f} EGP" if pd.notnull(x) else "N/A")
            display_df['kilometers'] = display_df['kilometers'].apply(
                lambda x: f"{x:,.0f} km" if pd.notnull(x) else "N/A")
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


if __name__ == "__main__":
    main()
