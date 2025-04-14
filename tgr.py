import streamlit as st
import pandas as pd
from google.cloud import bigquery
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import hashlib

# Authentication credentials
CREDENTIALS = {
    "admin": "8c6976e5b5410415bde908bd4dee15dfb167a9c873fc4bb8a81f6f2ab448a918",  # admin
    "user": "04f8996da763b7a969b1028ee3007569eaf3a635486ddab211d512c85b9df8fb",  # user
}


def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["username"] in CREDENTIALS and \
                hashlib.sha256(st.session_state["password"].encode()).hexdigest() == CREDENTIALS[
            st.session_state["username"]]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # Don't store the password
            del st.session_state["username"]  # Don't store the username
        else:
            st.session_state["password_correct"] = False

    # Return True if the password is validated
    if st.session_state.get("password_correct", False):
        return True

    # Show input fields for username and password
    st.text_input("Username", key="username")
    st.text_input("Password", type="password", key="password")
    st.button("Login", on_click=password_entered)

    if "password_correct" in st.session_state:
        st.error("😕 User not known or password incorrect")

    return False


# Set page config
st.set_page_config(
    page_title="SET - Sales Enablement Tool",
    page_icon="🚗",
    layout="wide"
)

# Main app logic
if check_password():
    # Function to load data from BigQuery
    @st.cache_data(ttl=43200)  # Cache data for 12 hours
    def load_data():
        try:
            # Create a BigQuery client
            client = bigquery.Client.from_service_account_json(st.secrets["service_account"])

            # Live cars query
            live_cars_query = """
            WITH sold AS ( 
                SELECT sf_vehicle_name 
                FROM `pricing-338819.ajans_dealers.dealer_requests`
                WHERE request_status = 'Payment Log' or wholesale_vehicle_sold_date is not null
            )
            SELECT avh.date_key, avh.sf_vehicle_name, avh.make, avh.model, avh.year, avh.kilometers 
            FROM `pricing-338819.reporting.ajans_vehicle_history` avh
            WHERE avh.date_key = current_date()
            AND avh.sf_vehicle_name NOT IN (SELECT sf_vehicle_name FROM sold)
            """

            # Historical data query
            historical_query = """
            WITH s AS (
                SELECT DISTINCT vehicle_id, 
                       DATE(wholesale_vehicle_sold_date) AS request_date, 
                       dealer_code,
                       dealer_name, 
                       dealer_phone,
                       car_name,
                       CASE 
                           WHEN discount_enabled IS TRUE THEN discounted_price 
                           ELSE buy_now_price 
                       END AS price
                FROM `pricing-338819.ajans_dealers.dealer_requests`
                WHERE request_type = 'Buy Now' 
                  AND wholesale_vehicle_sold_date IS NOT NULL
            ), p as (
                SELECT vehicle_id, min_published, minutes_published_for 
                FROM `pricing-338819.ajans_dealers.ajans_wholesale_to_retail_publishing_logs`
            ),
            cost as (
                SELECT DISTINCT car_name, sylndr_acquisition_price, market_retail_price, median_asked_price, refurbishment_cost 
                FROM `pricing-338819.reporting.daily_car_status`
            )

            SELECT s.request_date,s.dealer_code, round((p.minutes_published_for / 1440)) as time_on_app, s.price, c.make, c.model, c.year, c.kilometers,
                   cost.sylndr_acquisition_price, cost.market_retail_price,
                   s.dealer_name, s.dealer_phone
            FROM s 
            LEFT JOIN (
                SELECT  distinct ajans_vehicle_id, make, model, year, kilometers 
                FROM `pricing-338819.reporting.ajans_vehicle_history`
            ) AS c 
            ON s.vehicle_id = c.ajans_vehicle_id
            LEFT JOIN cost on s.car_name = cost.car_name
            LEFT JOIN p ON s.vehicle_id = p.vehicle_id
            """

            # Dealer segmentation query
            dealer_seg_query = """
           WITH dealer_purchases_60d AS (
    SELECT 
        dealer_code,
        dealer_name,
        DATE(wholesale_vehicle_sold_date) as purchase_date
    FROM ajans_dealers.dealer_requests
    WHERE request_type = 'Buy Now' 
    AND DATE(wholesale_vehicle_sold_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
),

dealer_purchases_previous_60d AS (
    SELECT 
        dealer_code,
        dealer_name,
        DATE(wholesale_vehicle_sold_date) as purchase_date
    FROM ajans_dealers.dealer_requests
    WHERE request_type = 'Buy Now' 
    AND DATE(wholesale_vehicle_sold_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
),

dealer_purchases_lifetime AS (
    SELECT 
        dealer_code,
        dealer_name,
        DATE(wholesale_vehicle_sold_date) as purchase_date
    FROM ajans_dealers.dealer_requests
    WHERE request_type = 'Buy Now' and wholesale_vehicle_sold_date is not null
),

dealer_requests_60d AS (
    SELECT 
        dealer_code,
        dealer_name,
        DATE(received_at) as request_date
    FROM ajans_dealers.dealer_requests
    WHERE request_type = 'Buy Now'
    AND DATE(received_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
),

dealer_requests_lifetime AS (
    SELECT 
        dealer_code,
        dealer_name,
        DATE(received_at) as request_date
    FROM ajans_dealers.dealer_requests
    WHERE request_type = 'Buy Now'
),

purchase_intervals_lifetime AS (
    SELECT 
        dealer_code,
        purchase_date,
        DATE_DIFF(purchase_date, 
            LAG(purchase_date) OVER (PARTITION BY dealer_code ORDER BY purchase_date),
            DAY) as days_between_purchases
    FROM dealer_purchases_lifetime
),

purchase_intervals_60d AS (
    SELECT 
        dealer_code,
        purchase_date,
        DATE_DIFF(purchase_date, 
            LAG(purchase_date) OVER (PARTITION BY dealer_code ORDER BY purchase_date),
            DAY) as days_between_purchases
    FROM dealer_purchases_60d
),

purchase_intervals_previous_60d AS (
    SELECT 
        dealer_code,
        purchase_date,
        DATE_DIFF(purchase_date, 
            LAG(purchase_date) OVER (PARTITION BY dealer_code ORDER BY purchase_date),
            DAY) as days_between_purchases
    FROM dealer_purchases_previous_60d
),

request_intervals_lifetime AS (
    SELECT 
        dealer_code,
        request_date,
        DATE_DIFF(request_date, 
            LAG(request_date) OVER (PARTITION BY dealer_code ORDER BY request_date),
            DAY) as days_between_requests
    FROM dealer_requests_lifetime
),

request_intervals_60d AS (
    SELECT 
        dealer_code,
        request_date,
        DATE_DIFF(request_date, 
            LAG(request_date) OVER (PARTITION BY dealer_code ORDER BY request_date),
            DAY) as days_between_requests
    FROM dealer_requests_60d
),

avg_intervals AS (
    SELECT 
        dealer_code,
        AVG(days_between_purchases) as lifetime_avg_days_between_purchases
    FROM purchase_intervals_lifetime
    WHERE days_between_purchases IS NOT NULL
    GROUP BY dealer_code
),

avg_intervals_60d AS (
    SELECT 
        dealer_code,
        AVG(days_between_purchases) as sixty_day_avg_days_between_purchases
    FROM purchase_intervals_60d
    WHERE days_between_purchases IS NOT NULL
    GROUP BY dealer_code
),

avg_intervals_previous_60d AS (
    SELECT 
        dealer_code,
        AVG(days_between_purchases) as previous_sixty_day_avg_days_between_purchases
    FROM purchase_intervals_previous_60d
    WHERE days_between_purchases IS NOT NULL
    GROUP BY dealer_code
),

avg_request_intervals AS (
    SELECT 
        dealer_code,
        AVG(days_between_requests) as lifetime_avg_days_between_requests
    FROM request_intervals_lifetime
    WHERE days_between_requests IS NOT NULL
    GROUP BY dealer_code
),

avg_request_intervals_60d AS (
    SELECT 
        dealer_code,
        AVG(days_between_requests) as sixty_day_avg_days_between_requests
    FROM request_intervals_60d
    WHERE days_between_requests IS NOT NULL
    GROUP BY dealer_code
),

dealer_requests_30d AS (
    SELECT 
        dealer_code,
        COUNT(*) as buy_requests_30d
    FROM ajans_dealers.dealer_requests
    WHERE request_type = 'Buy Now'
    AND DATE(received_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY dealer_code
),

sold_cars_30d AS (
    SELECT 
        dealer_code,
        COUNT(*) as sold_cars_30d
    FROM ajans_dealers.dealer_requests
    WHERE request_type = 'Buy Now'
    AND DATE(wholesale_vehicle_sold_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY dealer_code
),

last_purchase_dates AS (
    SELECT 
        dealer_code,
        MAX(DATE(wholesale_vehicle_sold_date)) as last_purchase_date,
        DATE_DIFF(CURRENT_DATE(), MAX(DATE(wholesale_vehicle_sold_date)), DAY) as days_since_last_purchase
    FROM ajans_dealers.dealer_requests
    WHERE request_type = 'Buy Now' 
    AND wholesale_vehicle_sold_date IS NOT NULL
    GROUP BY dealer_code
),

last_purchase_dates_previous_60d AS (
    SELECT 
        dealer_code,
        MAX(DATE(wholesale_vehicle_sold_date)) as last_purchase_date_previous_60d,
        DATE_DIFF(DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY), 
                 MAX(DATE(wholesale_vehicle_sold_date)), DAY) as days_since_last_purchase_previous_60d
    FROM ajans_dealers.dealer_requests
    WHERE request_type = 'Buy Now' 
    AND DATE(wholesale_vehicle_sold_date) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 120 DAY) AND DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
    GROUP BY dealer_code
),

metrics AS (
    SELECT 
        d.dealer_code,
        d.dealer_name,
        CASE 
            WHEN COUNT(DISTINCT pl.purchase_date) > 0 THEN 'Dealer'
            ELSE 'User'
        END as user_vs_dealer_flag,
        ai.lifetime_avg_days_between_purchases,
        ai60.sixty_day_avg_days_between_purchases,
        aip60.previous_sixty_day_avg_days_between_purchases,
        ari.lifetime_avg_days_between_requests,
        ari60.sixty_day_avg_days_between_requests,
        lpd.last_purchase_date,
        lpd.days_since_last_purchase,
        lpdp.last_purchase_date_previous_60d,
        lpdp.days_since_last_purchase_previous_60d,
        COUNT(DISTINCT pl.purchase_date) as total_purchases_lifetime,
        COUNT(DISTINCT p60.purchase_date) as total_purchases_60d,
        COUNT(DISTINCT pp60.purchase_date) as total_purchases_previous_60d,
        COUNT(DISTINCT rl.request_date) as total_requests_lifetime,
        COUNT(DISTINCT r60.request_date) as total_requests_60d,
        ROUND(COUNT(DISTINCT pl.purchase_date) / NULLIF(COUNT(DISTINCT DATE_TRUNC(pl.purchase_date, MONTH)), 0), 2) as avg_purchases_per_month_lifetime,
        ROUND(COUNT(DISTINCT p60.purchase_date) / NULLIF(COUNT(DISTINCT DATE_TRUNC(p60.purchase_date, MONTH)), 0), 2) as avg_purchases_per_month_60d,
        ROUND(COUNT(DISTINCT pp60.purchase_date) / NULLIF(COUNT(DISTINCT DATE_TRUNC(pp60.purchase_date, MONTH)), 0), 2) as avg_purchases_per_month_previous_60d,
        ROUND(COUNT(DISTINCT rl.request_date) / NULLIF(COUNT(DISTINCT DATE_TRUNC(rl.request_date, MONTH)), 0), 2) as avg_requests_per_month_lifetime,
        ROUND(COUNT(DISTINCT r60.request_date) / NULLIF(COUNT(DISTINCT DATE_TRUNC(r60.request_date, MONTH)), 0), 2) as avg_requests_per_month_60d,
        sc.sold_cars_30d,
        dr30.buy_requests_30d
    FROM ajans_dealers.dealers d
    LEFT JOIN dealer_purchases_lifetime pl ON d.dealer_code = pl.dealer_code
    LEFT JOIN dealer_purchases_60d p60 ON d.dealer_code = p60.dealer_code
    LEFT JOIN dealer_purchases_previous_60d pp60 ON d.dealer_code = pp60.dealer_code
    LEFT JOIN dealer_requests_lifetime rl ON d.dealer_code = rl.dealer_code
    LEFT JOIN dealer_requests_60d r60 ON d.dealer_code = r60.dealer_code
    LEFT JOIN avg_intervals ai ON d.dealer_code = ai.dealer_code
    LEFT JOIN avg_intervals_60d ai60 ON d.dealer_code = ai60.dealer_code
    LEFT JOIN avg_intervals_previous_60d aip60 ON d.dealer_code = aip60.dealer_code
    LEFT JOIN avg_request_intervals ari ON d.dealer_code = ari.dealer_code
    LEFT JOIN avg_request_intervals_60d ari60 ON d.dealer_code = ari60.dealer_code
    LEFT JOIN sold_cars_30d sc ON d.dealer_code = sc.dealer_code
    LEFT JOIN dealer_requests_30d dr30 ON d.dealer_code = dr30.dealer_code
    LEFT JOIN last_purchase_dates lpd ON d.dealer_code = lpd.dealer_code
    LEFT JOIN last_purchase_dates_previous_60d lpdp ON d.dealer_code = lpdp.dealer_code
    GROUP BY 
        d.dealer_code,
        d.dealer_name,
        ai.lifetime_avg_days_between_purchases,
        ai60.sixty_day_avg_days_between_purchases,
        aip60.previous_sixty_day_avg_days_between_purchases,
        ari.lifetime_avg_days_between_requests,
        ari60.sixty_day_avg_days_between_requests,
        lpd.last_purchase_date,
        lpd.days_since_last_purchase,
        lpdp.last_purchase_date_previous_60d,
        lpdp.days_since_last_purchase_previous_60d,
        sc.sold_cars_30d,
        dr30.buy_requests_30d
)

SELECT 
    *,
    CASE 
        WHEN total_purchases_lifetime = 0 THEN 'No Purchase'
        WHEN total_purchases_lifetime = 1 THEN '1 Time Purchaser'
        WHEN lifetime_avg_days_between_purchases <= 15 THEN '0-15 days'
        WHEN lifetime_avg_days_between_purchases <= 30 THEN '15-30 days'
        WHEN lifetime_avg_days_between_purchases <= 60 THEN '30-60 days'
        ELSE '60+ days'
    END as purchase_segment_lifetime,
    CASE 
        WHEN total_purchases_60d = 0 THEN 'No Purchase'
        WHEN total_purchases_60d = 1 THEN '1 Time Purchaser'
        WHEN sixty_day_avg_days_between_purchases <= 15 THEN 
            CASE
                WHEN sixty_day_avg_days_between_purchases <= 7.5 THEN '0-15 days (New)'
                ELSE '0-15 days (At Risk)'
            END
        WHEN sixty_day_avg_days_between_purchases <= 30 THEN 
            CASE
                WHEN sixty_day_avg_days_between_purchases <= 22.5 THEN '15-30 days (New)'
                ELSE '15-30 days (At Risk)'
            END
        WHEN sixty_day_avg_days_between_purchases <= 60 THEN 
            CASE
                WHEN sixty_day_avg_days_between_purchases <= 45 THEN '30-60 days (New)'
                ELSE '30-60 days (At Risk)'
            END
        ELSE '60+ days'
    END as purchase_segment_60d,
    CASE 
        WHEN total_purchases_previous_60d = 0 THEN 'No Purchase'
        WHEN total_purchases_previous_60d = 1 THEN '1 Time Purchaser'
        WHEN previous_sixty_day_avg_days_between_purchases <= 15 THEN '0-15 days'
        WHEN previous_sixty_day_avg_days_between_purchases <= 30 THEN '15-30 days'
        WHEN previous_sixty_day_avg_days_between_purchases <= 60 THEN '30-60 days'
        ELSE '60+ days'
    END as purchase_segment_previous_60d,
    CASE 
        WHEN total_purchases_lifetime = 0 THEN 'No Purchase'
        WHEN total_purchases_lifetime = 1 THEN '1 Time Purchaser'
        WHEN lifetime_avg_days_between_purchases <= 30 THEN 'Frequent'
        WHEN lifetime_avg_days_between_purchases <= 60 THEN 'Active'
        ELSE 'Inactive'
    END as final_bucket_lifetime,
    CASE 
        WHEN total_purchases_60d = 0 THEN 'No Purchase'
        WHEN total_purchases_60d = 1 THEN '1 Time Purchaser'
        WHEN sixty_day_avg_days_between_purchases <= 30 THEN 
            CASE
                WHEN sixty_day_avg_days_between_purchases <= 15 THEN 'Frequent (New)'
                ELSE 'Frequent (At Risk)'
            END
        WHEN sixty_day_avg_days_between_purchases <= 60 THEN 
            CASE
                WHEN sixty_day_avg_days_between_purchases <= 45 THEN 'Active (New)'
                ELSE 'Active (At Risk)'
            END
        ELSE 'Inactive'
    END as final_bucket_60d,
    CASE 
        WHEN total_purchases_previous_60d = 0 THEN 'No Purchase'
        WHEN total_purchases_previous_60d = 1 THEN '1 Time Purchaser'
        WHEN previous_sixty_day_avg_days_between_purchases <= 30 THEN 'Frequent'
        WHEN previous_sixty_day_avg_days_between_purchases <= 60 THEN 'Active'
        ELSE 'Inactive'
    END as final_bucket_previous_60d,
    CASE
        WHEN total_purchases_previous_60d = 0 AND total_purchases_60d = 0 THEN 'No Purchase'
        WHEN total_purchases_previous_60d = 0 AND total_purchases_60d = 1 THEN 'No Purchase - 1 Time Purchaser'
        WHEN total_purchases_previous_60d = 1 AND total_purchases_60d = 0 THEN '1 Time Purchaser - No Purchase'
        ELSE 
            CONCAT(
                CASE 
                    WHEN total_purchases_previous_60d = 0 THEN 'No Purchase'
                    WHEN previous_sixty_day_avg_days_between_purchases <= 30 THEN 'Frequent'
                    WHEN previous_sixty_day_avg_days_between_purchases <= 60 THEN 'Active'
                    ELSE 'Inactive'
                END,
                ' - ',
                CASE 
                    WHEN total_purchases_60d = 0 THEN 'No Purchase'
                    WHEN total_purchases_60d = 1 THEN '1 Time Purchaser'
                    WHEN sixty_day_avg_days_between_purchases <= 30 THEN 
                        CASE
                            WHEN days_since_last_purchase <= 15 THEN 
                                CASE 
                                    WHEN previous_sixty_day_avg_days_between_purchases > 30 
                                         OR previous_sixty_day_avg_days_between_purchases IS NULL THEN 'Frequent (New)'
                                    ELSE 'Frequent'
                                END
                            ELSE 'Frequent (At Risk)'
                        END
                    WHEN sixty_day_avg_days_between_purchases <= 60 THEN 
                        CASE
                            WHEN days_since_last_purchase <= 30 THEN 
                                CASE 
                                    WHEN previous_sixty_day_avg_days_between_purchases > 60 
                                         OR previous_sixty_day_avg_days_between_purchases IS NULL THEN 'Active (New)'
                                    ELSE 'Active'
                                END
                            ELSE 'Active (At Risk)'
                        END
                    ELSE 'Inactive'
                END
            )
    END as current_segmentation,
    CASE 
        WHEN total_requests_lifetime = 0 THEN 'No Requests'
        WHEN total_requests_lifetime = 1 THEN '1 Request'
        WHEN lifetime_avg_days_between_requests <= 5 THEN '0-5 days'
        WHEN lifetime_avg_days_between_requests <= 10 THEN '5-10 days'
        WHEN lifetime_avg_days_between_requests <= 21 THEN '10-21 days'
        ELSE '21+ days'
    END as request_segment_lifetime,
    CASE 
        WHEN total_requests_60d = 0 THEN 'No Requests'
        WHEN total_requests_60d = 1 THEN '1 Request'
        WHEN sixty_day_avg_days_between_requests <= 5 THEN '0-5 days'
        WHEN sixty_day_avg_days_between_requests <= 10 THEN '5-10 days'
        WHEN sixty_day_avg_days_between_requests <= 21 THEN '10-21 days'
        ELSE '21+ days'
    END as request_segment_60d,
    CASE 
        WHEN total_requests_lifetime = 0 THEN 'No Requests'
        WHEN total_requests_lifetime = 1 THEN '1 Request'
        WHEN lifetime_avg_days_between_requests <= 10 THEN 'Frequent'
        WHEN lifetime_avg_days_between_requests <= 21 THEN 'Active'
        ELSE 'Inactive'
    END as request_activity_bucket_lifetime,
    CASE 
        WHEN total_requests_60d = 0 THEN 'No Requests'
        WHEN total_requests_60d = 1 THEN '1 Request'
        WHEN sixty_day_avg_days_between_requests <= 10 THEN 'Frequent'
        WHEN sixty_day_avg_days_between_requests <= 21 THEN 'Active'
        ELSE 'Inactive'
    END as request_activity_bucket_60d
FROM metrics
ORDER BY total_requests_lifetime DESC NULLS LAST;
            """

            # Dealer activity query
            dealer_activity_query = """
            WITH 
            -- Dealers active in last 30 days
            active_dealers_30d AS (
                SELECT 
                    dealer_code,
                    dealer_name,
                    COUNT(DISTINCT event_date) as active_days_30d,
                    SUM(all_cars_events) as total_car_events_30d
                FROM `pricing-338819.ajans_dealers.dealers_activity`
                WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                AND all_cars_events > 0
                GROUP BY dealer_code, dealer_name
            ),

            -- Dealers active in last 7 days
            active_dealers_7d AS (
                SELECT 
                    dealer_code,
                    dealer_name,
                    COUNT(DISTINCT event_date) as active_days_7d,
                    SUM(all_cars_events) as total_car_events_7d
                FROM `pricing-338819.ajans_dealers.dealers_activity`
                WHERE event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
                AND all_cars_events > 0
                GROUP BY dealer_code, dealer_name
            )

            -- Combine results
            SELECT 
                COALESCE(d30.dealer_code, d7.dealer_code) as dealer_code,
                COALESCE(d30.dealer_name, d7.dealer_name) as dealer_name,
                COALESCE(d30.active_days_30d, 0) as active_days_30d,
                COALESCE(d30.total_car_events_30d, 0) as total_car_events_30d,
                COALESCE(d7.active_days_7d, 0) as active_days_7d,
                COALESCE(d7.total_car_events_7d, 0) as total_car_events_7d
            FROM active_dealers_30d d30
            FULL OUTER JOIN active_dealers_7d d7 
                ON d30.dealer_code = d7.dealer_code
            ORDER BY 
                active_days_30d DESC,
                total_car_events_30d DESC
            """

            # Execute queries
            live_cars_df = client.query(live_cars_query).to_dataframe()
            historical_df = client.query(historical_query).to_dataframe()
            print(historical_df)
            dealer_seg_df = client.query(dealer_seg_query).to_dataframe()
            activity_df = client.query(dealer_activity_query).to_dataframe()

            # Convert date columns
            historical_df['request_date'] = pd.to_datetime(historical_df['request_date'])
            live_cars_df['date_key'] = pd.to_datetime(live_cars_df['date_key'])

            # Convert numeric columns
            numeric_columns = ['time_on_app', 'price', 'year', 'kilometers', 'sylndr_acquisition_price',
                               'market_retail_price', 'median_asked_price', 'refurbishment_cost']
            for col in numeric_columns:
                if col in historical_df.columns:
                    historical_df[col] = pd.to_numeric(historical_df[col], errors='coerce')

            live_cars_df['year'] = pd.to_numeric(live_cars_df['year'], errors='coerce')
            live_cars_df['kilometers'] = pd.to_numeric(live_cars_df['kilometers'], errors='coerce')

            return historical_df, live_cars_df, dealer_seg_df, activity_df

        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


    def get_dealers_needing_attention(dealer_seg_df):
        """Identify dealers who need attention based on their current segmentation and transitions."""

        attention_needed = dealer_seg_df.copy()

        def get_priority(row):
            current_seg = row['current_segmentation']

            # Critical Priority Cases
            if any([
                'Frequent - No Purchase' in current_seg,
                'Active - No Purchase' in current_seg,
                'Frequent - Active (At Risk)' in current_seg,
                'Frequent - Frequent (At Risk)' in current_seg
            ]):
                return '🔴 Critical Priority'

            # High Priority Cases
            elif any([
                'Active - Active (At Risk)' in current_seg,
                'Frequent - Active' in current_seg,
                'Active - Frequent (At Risk)' in current_seg,
                'Inactive - Active (At Risk)' in current_seg,
                'Inactive - Frequent (At Risk)' in current_seg
            ]):
                return '🟠 High Priority'

            # Medium Priority Cases
            elif any([
                'No Purchase - Active' in current_seg,
                'No Purchase - Frequent' in current_seg,
                'Inactive - Active' in current_seg,
                'Inactive - Frequent' in current_seg,
                '1 Time Purchaser - No Purchase' in current_seg
            ]):
                return '🟡 Medium Priority'

            # Low Priority Cases
            else:
                return '⚪ Low Priority'

        # Add priority based on current segmentation
        attention_needed['status'] = attention_needed.apply(get_priority, axis=1)

        # Filter out low priority cases
        attention_needed = attention_needed[attention_needed['status'] != '⚪ Low Priority'].copy()

        # Calculate activity metrics
        attention_needed['activity_drop'] = (
                attention_needed['avg_requests_per_month_lifetime'] -
                attention_needed['avg_requests_per_month_60d']
        )

        # Sort by priority (Critical -> High -> Medium) and then by activity drop
        priority_order = ['🔴 Critical Priority', '🟠 High Priority', '🟡 Medium Priority']
        return attention_needed.sort_values(
            ['status', 'activity_drop'],
            ascending=[True, False],
            key=lambda x: pd.Categorical(x, categories=priority_order) if x.name == 'status' else x
        )


    def main():
        st.title("🚗 SET - Sales Enablement Tool")

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
                            f"{dealer['status']} - {dealer['dealer_name']}"
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
            valid_dealers = sorted(
                set(dealer_seg_df['dealer_name'].unique()) & set(activity_df['dealer_name'].unique()))

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
            # st.subheader("Recommended Cars")
            dealer_historical = historical_df[historical_df['dealer_name'] == selected_dealer].copy()

            # if not dealer_historical.empty:
            #     recommended_cars = get_recommended_cars(dealer_historical, live_cars_df)
            #
            #     if not recommended_cars.empty:
            #         st.dataframe(
            #             recommended_cars,
            #             column_config={
            #                 "match_score": st.column_config.ProgressColumn(
            #                     "Match Score",
            #                     help="How well this car matches the dealer's preferences (max 9 points)",
            #                     format="%.1f",
            #                     min_value=0,
            #                     max_value=9
            #                 ),
            #                 "score_breakdown": "Score Breakdown",
            #                 "sf_vehicle_name": "Vehicle",
            #                 "make": "Make",
            #                 "model": "Model",
            #                 "year": "Year",
            #                 "kilometers": st.column_config.NumberColumn(
            #                     "Mileage",
            #                     format="%d km"
            #                 )
            #             },
            #             use_container_width=True
            #         )
            #
            #         # Add explanation of scoring system
            #         st.info("""
            #         **Scoring System:**
            #         - Make: 0-3 points (based on frequency of purchases)
            #         - Model: 0-2 points (based on frequency within make)
            #         - Year: 0-2 points (based on preferred year ranges)
            #         - Mileage: 0-2 points (based on preferred km ranges)
            #         Total possible score: 9 points
            #         """)
            #     else:
            #         st.info("No matching cars found in current inventory")
            # else:
            #     st.info("Cannot generate recommendations without historical data")

            # Historical Purchase Analysis (now after Recommended Cars)
            st.subheader("Historical Purchase Analysis")

            if not dealer_historical.empty:
                # Clean the data for visualization
                dealer_historical['kilometers'] = dealer_historical['kilometers'].fillna(
                    dealer_historical['kilometers'].mean())

                # Create tabs for different visualizations
                hist_tab1, hist_tab2, hist_tab3 = st.tabs(
                    ["Purchase Timeline", "Price Distribution", "Make Distribution"])

                with hist_tab1:
                    # Create time series of purchases with better handling of missing values
                    fig = px.scatter(dealer_historical,
                                     x='request_date',
                                     y='price',
                                     color='make',
                                     size='kilometers',
                                     hover_data=['model', 'year'],
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
                    ['request_date', 'make', 'model', 'year', 'kilometers', 'price']].copy()

                # Format numeric columns
                display_df['price'] = display_df['price'].apply(lambda x: f"{x:,.0f} EGP" if pd.notnull(x) else "N/A")
                display_df['kilometers'] = display_df['kilometers'].apply(
                    lambda x: f"{x:,.0f} km" if pd.notnull(x) else "N/A")

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
                        "price": "Price"
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

            else:
                st.warning("No historical purchase data available for this dealer")


    if __name__ == "__main__":
        main()
