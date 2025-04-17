import streamlit as st
import pandas as pd
from google.cloud import bigquery
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import hashlib
import json
from google.oauth2 import service_account

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
        # Try to get credentials from Streamlit secrets first
        try:
            credentials = service_account.Credentials.from_service_account_info(
                st.secrets["service_account"]
            )
        except (KeyError, FileNotFoundError):
            # If secret not found, try to use service_account.json
            try:
                credentials = service_account.Credentials.from_service_account_file(
                    'service_account.json'
                )
            except FileNotFoundError:
                st.error(
                    "No credentials found. Please configure either Streamlit secrets or provide a service_account.json file.")
                return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

        # Create a BigQuery client using the credentials
        client = bigquery.Client(credentials=credentials)

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

        # New query for recent car views
        recent_views_query = """
        SELECT 
            time,
            make,
            model,
            trim,
            year,
            kilometrage,
            transmission,
            listing_title,
            buy_now_price,
            body_style,
            entity_code as dealer_code
        FROM `pricing-338819.silver_ajans_mixpanel.screen_car_profile_event`
        WHERE DATE(time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND entity_code IS NOT NULL
        ORDER BY time DESC
        """

        # New query for recent filters
        recent_filters_query = """
        SELECT 
            time,
            make,
            model,
            year,
            kilometrage,
            group_filter,
            status,
            no_of_cars,
            entity_code as dealer_code
        FROM `pricing-338819.silver_ajans_mixpanel.action_filter`
        WHERE DATE(time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        AND entity_code IS NOT NULL
        ORDER BY time DESC
        """

        # Execute all queries
        live_cars_df = client.query(live_cars_query).to_dataframe()
        historical_df = client.query(historical_query).to_dataframe()
        dealer_seg_df = client.query(dealer_seg_query).to_dataframe()
        activity_df = client.query(dealer_activity_query).to_dataframe()
        recent_views_df = client.query(recent_views_query).to_dataframe()
        recent_filters_df = client.query(recent_filters_query).to_dataframe()

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

        return historical_df, live_cars_df, dealer_seg_df, activity_df, recent_views_df, recent_filters_df


    # Define priority cases at the module level
    critical_cases = [
        'Active - Active (At Risk)',
        'Active - Frequent (At Risk)',
        'Frequent - Frequent (At Risk)',
        'Frequent - No Purchase',
        'Frequent - Active (At Risk)',
        'Inactive - Active (At Risk)',
        'Inactive - Frequent (At Risk)',
        'Inactive - Frequent',
        'No Purchase - Active (At Risk)',
        'No Purchase - 1 Time Purchaser',
        'No Purchase - Frequent (At Risk)'
    ]

    high_cases = [
        'Active - No Purchase',
        'Active - Frequent',
        'Active - Inactive',
        'Frequent - Active',
        'Frequent - Inactive',
        'Frequent - 1 Time Purchaser',
        'Inactive - Active',
        'No Purchase - Active',
        'No Purchase - Frequent'
    ]

    medium_cases = [
        'Active - 1 Time Purchaser',
        'Active - Active',
        'Active - Frequent (New)',
        'Active - Active (New)',
        'Frequent - Frequent',
        'Frequent - Active (New)',
        'Frequent - Frequent (New)',
        'Inactive - Active (New)',
        'Inactive - Frequent (New)',
        'Inactive - Inactive',
        'Inactive - No Purchase',
        'Inactive - 1 Time Purchaser',
        'No Purchase - Active (New)',
        'No Purchase - Frequent (New)'
    ]

    low_cases = [
        '1 Time Purchaser - No Purchase',
        'No Purchase - Inactive',
        'No Purchase - No Purchase'
    ]


    def get_priority(row):
        """Determine priority level based on current segmentation."""
        current_seg = row['current_segmentation']

        if any(case in current_seg for case in critical_cases):
            return '🔴 Critical Priority'
        elif any(case in current_seg for case in high_cases):
            return '🟠 High Priority'
        elif any(case in current_seg for case in medium_cases):
            return '🟡 Medium Priority'
        else:
            return '⚪ Low Priority'


    def get_dealers_needing_attention(dealer_seg_df):
        """Identify dealers who need attention based on their current segmentation and transitions."""

        # Filter out users, keep only dealers
        attention_needed = dealer_seg_df[dealer_seg_df['user_vs_dealer_flag'] == 'Dealer'].copy()

        # Add priority based on current segmentation
        attention_needed['status'] = attention_needed.apply(get_priority, axis=1)

        # Calculate activity metrics
        attention_needed['activity_drop'] = (
                attention_needed['avg_requests_per_month_lifetime'] -
                attention_needed['avg_requests_per_month_60d']
        )

        # Sort by priority (Critical -> High -> Medium -> Low) and then by activity drop
        priority_order = ['🔴 Critical Priority', '🟠 High Priority', '🟡 Medium Priority', '⚪ Low Priority']

        def get_priority_score(row):
            current_seg = row['current_segmentation']

            # Get the list of cases for the row's priority level
            if row['status'] == '🔴 Critical Priority':
                cases = critical_cases
            elif row['status'] == '🟠 High Priority':
                cases = high_cases
            elif row['status'] == '🟡 Medium Priority':
                cases = medium_cases
            else:
                cases = low_cases

            # Find the index of the case in its priority level list
            for i, case in enumerate(cases):
                if case in current_seg:
                    return i
            return len(cases)  # If not found, put at end of its priority group

        attention_needed['priority_score'] = attention_needed.apply(get_priority_score, axis=1)

        return attention_needed.sort_values(
            ['status', 'priority_score', 'activity_drop'],
            ascending=[True, True, False],
            key=lambda x: pd.Categorical(x, categories=priority_order) if x.name == 'status' else x
        )


    def get_recommended_cars(dealer_historical, live_cars):
        """
        Generate car recommendations based on dealer's historical purchases.
        Returns a DataFrame with recommended cars and their match scores.
        """
        if dealer_historical.empty or live_cars.empty:
            return pd.DataFrame()

        # Calculate make preferences
        make_counts = dealer_historical['make'].value_counts()
        total_purchases = len(dealer_historical)
        make_scores = make_counts / total_purchases * 3  # Scale to 0-3 points

        # Calculate model preferences within each make
        model_preferences = {}
        for make in make_counts.index:
            make_data = dealer_historical[dealer_historical['make'] == make]
            model_counts = make_data['model'].value_counts()
            model_preferences[make] = model_counts / len(make_data) * 2  # Scale to 0-2 points

        # Calculate year and mileage ranges
        year_mean = dealer_historical['year'].mean()
        year_std = dealer_historical['year'].std()
        km_mean = dealer_historical['kilometers'].mean()
        km_std = dealer_historical['kilometers'].std()

        # Score each available car
        scored_cars = []
        for _, car in live_cars.iterrows():
            # Initialize score components
            make_score = make_scores.get(car['make'], 0)
            model_score = model_preferences.get(car['make'], pd.Series()).get(car['model'], 0)

            # Year score (0-2 points)
            year_diff = abs(car['year'] - year_mean)
            year_score = max(0, 2 - (year_diff / year_std)) if year_std > 0 else 0
            year_score = min(2, max(0, year_score))  # Clamp between 0 and 2

            # Kilometer score (0-2 points)
            km_diff = abs(car['kilometers'] - km_mean)
            km_score = max(0, 2 - (km_diff / km_std)) if km_std > 0 else 0
            km_score = min(2, max(0, km_score))  # Clamp between 0 and 2

            # Total score
            total_score = make_score + model_score + year_score + km_score

            # Create score breakdown
            score_breakdown = f"Make: {make_score:.1f}, Model: {model_score:.1f}, Year: {year_score:.1f}, Mileage: {km_score:.1f}"

            # Add to results
            scored_cars.append({
                'sf_vehicle_name': car['sf_vehicle_name'],
                'make': car['make'],
                'model': car['model'],
                'year': car['year'],
                'kilometers': car['kilometers'],
                'match_score': total_score,
                'score_breakdown': score_breakdown
            })

        # Convert to DataFrame and sort by score
        recommendations = pd.DataFrame(scored_cars)
        if not recommendations.empty:
            recommendations = recommendations.sort_values('match_score', ascending=False)

        return recommendations


    def get_olx_listings_for_dealer(client, dealer_id):
        """
        Get OLX listings for a specific dealer from the last 30 days.
        Args:
            client: BigQuery client
            dealer_id: The dealer's code to search for
        Returns:
            DataFrame containing the dealer's OLX listings
        """
        olx_query = """
        WITH cleaned_numbers AS (
            SELECT
                DISTINCT seller_name,
                REGEXP_REPLACE(seller_phone_number, r'[^0-9,]', '') AS cleaned_phone_number,
                id,
                title,
                transmission_type,
                year,
                kilometers,
                make,
                model,
                payment_options,
                condition,
                engine_capacity,
                extra_features,
                color,
                body_type,
                ad_type,
                fuel_type,
                description,
                images,
                region,
                price,
                is_active,
                added_at,
                deactivated_at,
                is_dealer,
                created_at
            FROM olx.listings
            WHERE added_at >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        ),
        split_numbers AS (
            SELECT
                *,
                SPLIT(cleaned_phone_number, ',') AS phone_numbers
            FROM cleaned_numbers
        ),
        flattened_numbers AS (
            SELECT
                DISTINCT
                id,
                title,
                transmission_type,
                year,
                kilometers,
                make,
                model,
                payment_options,
                condition,
                engine_capacity,
                extra_features,
                color,
                body_type,
                ad_type,
                fuel_type,
                description,
                images,
                region,
                price,
                seller_name,
                is_active,
                added_at,
                deactivated_at,
                is_dealer,
                created_at,
                SUBSTR(phone_number, 2) AS phone_number
            FROM split_numbers,
            UNNEST(phone_numbers) AS phone_number
        )

        SELECT 
            f.*,
            d.dealer_name,
            d.dealer_code,
            d.dealer_status,
            d.dealer_email,
            d.branch_city,
            d.dealer_account_manager_name,
            d.dealer_account_manager_email
        FROM flattened_numbers f
        INNER JOIN gold_wholesale.dim_dealers d
        ON f.phone_number = d.dealer_phone
        WHERE d.dealer_code = @dealer_id
        ORDER BY added_at DESC
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dealer_id", "STRING", dealer_id)
            ]
        )

        try:
            df = client.query(olx_query, job_config=job_config).to_dataframe()
            return df
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame()


    def get_dealer_requests(client, dealer_code):
        """
        Get dealer requests data for a specific dealer.
        Returns four dataframes: all requests, succeeded, failed before visit, and failed after visit requests.
        """
        # Query for all requests
        all_requests_query = """
        SELECT 
            dealer_code,
            vehicle_request_created_at,
            request_type,
            request_status,
            contacted_at,
            contacted_user,
            visited_at,
            visited_user,
            succeeded_at,
            failed_before_visit_at,
            failed_after_visit_at,
            failure_reason,
            car_name,
            car_make,
            car_model,
            car_year,
            car_kilometrage,
            buy_now_price,
            discounted_price
        FROM `pricing-338819.ajans_dealers.dealer_requests` 
        WHERE dealer_code = @dealer_id 
        ORDER BY vehicle_request_created_at DESC
        LIMIT 10
        """

        # Query for succeeded requests
        succeeded_query = """
        SELECT 
            dealer_code,
            vehicle_request_created_at,
            request_type,
            contacted_at,
            contacted_user,
            visited_at,
            visited_user,
            succeeded_at,
            car_name,
            car_make,
            car_model,
            car_year,
            car_kilometrage,
            buy_now_price,
            discounted_price
        FROM `pricing-338819.ajans_dealers.dealer_requests` 
        WHERE dealer_code = @dealer_id 
        AND request_status = 'Succeeded'
        ORDER BY vehicle_request_created_at DESC
        LIMIT 10
        """

        # Query for failed before visit requests
        failed_before_query = """
        SELECT 
            dealer_code,
            vehicle_request_created_at,
            request_type,
            contacted_at,
            contacted_user,
            failed_before_visit_at,
            failure_reason,
            car_name,
            car_make,
            car_model,
            car_year,
            car_kilometrage,
            buy_now_price,
            discounted_price
        FROM `pricing-338819.ajans_dealers.dealer_requests` 
        WHERE dealer_code = @dealer_id 
        AND request_status = 'Failed Before Visit'
        ORDER BY vehicle_request_created_at DESC
        LIMIT 10
        """

        # Query for failed after visit requests
        failed_after_query = """
        SELECT 
            dealer_code,
            vehicle_request_created_at,
            request_type,
            contacted_at,
            contacted_user,
            visited_at,
            visited_user,
            failed_after_visit_at,
            failure_reason,
            car_name,
            car_make,
            car_model,
            car_year,
            car_kilometrage,
            buy_now_price,
            discounted_price
        FROM `pricing-338819.ajans_dealers.dealer_requests` 
        WHERE dealer_code = @dealer_id 
        AND request_status = 'Failed After Visit'
        ORDER BY vehicle_request_created_at DESC
        LIMIT 10
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("dealer_id", "STRING", dealer_code)
            ]
        )

        try:
            all_requests_df = client.query(all_requests_query, job_config=job_config).to_dataframe()
            succeeded_df = client.query(succeeded_query, job_config=job_config).to_dataframe()
            failed_before_df = client.query(failed_before_query, job_config=job_config).to_dataframe()
            failed_after_df = client.query(failed_after_query, job_config=job_config).to_dataframe()

            # Format datetime columns
            datetime_columns = ['vehicle_request_created_at', 'contacted_at', 'visited_at',
                                'succeeded_at', 'failed_before_visit_at', 'failed_after_visit_at']

            for df in [all_requests_df, succeeded_df, failed_before_df, failed_after_df]:
                for col in datetime_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col]).dt.strftime('%Y-%m-%d %H:%M:%S')

            # Format price columns
            if 'buy_now_price' in df.columns:
                df['buy_now_price'] = df['buy_now_price'].apply(
                    lambda x: f"EGP {x:,.0f}" if pd.notnull(x) else "N/A"
                )
            if 'discounted_price' in df.columns:
                df['discounted_price'] = df['discounted_price'].apply(
                    lambda x: f"EGP {x:,.0f}" if pd.notnull(x) else "N/A"
                )

            # Format kilometrage
            if 'car_kilometrage' in df.columns:
                df['car_kilometrage'] = df['car_kilometrage'].apply(
                    lambda x: f"{x:,.0f} km" if pd.notnull(x) else "N/A"
                )

            return all_requests_df, succeeded_df, failed_before_df, failed_after_df
        except Exception as e:
            print(f"Error executing query: {e}")
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


    @st.dialog("Makes Distribution Analysis")
    def show_makes_analysis(all_makes):
        # Create two columns
        make_col1, make_col2 = st.columns(2)
        with make_col1:
            st.write("**Top Makes:**")
            for make, count in all_makes.items():
                st.write(f"• {make}: {count} purchases")
        with make_col2:
            # Create a pie chart of makes
            fig = px.pie(
                values=all_makes.values,
                names=all_makes.index,
                title='Makes Distribution'
            )
            st.plotly_chart(fig, use_container_width=True)


    @st.dialog("Mileage Distribution Analysis")
    def show_mileage_analysis(all_mileage):
        # Create two columns
        mileage_col1, mileage_col2 = st.columns(2)
        with mileage_col1:
            st.write("**Mileage Ranges:**")
            for range_, count in all_mileage.items():
                st.write(f"• {range_}: {count} purchases")
        with mileage_col2:
            # Create a bar chart of mileage ranges
            fig = px.bar(
                x=all_mileage.index,
                y=all_mileage.values,
                title='Mileage Distribution',
                labels={'x': 'Mileage Range', 'y': 'Number of Purchases'}
            )
            st.plotly_chart(fig, use_container_width=True)


    @st.dialog("Models Distribution Analysis")
    def show_models_analysis(all_models):
        # Create two columns
        model_col1, model_col2 = st.columns(2)
        with model_col1:
            st.write("**Top Models:**")
            for (make, model), count in all_models.head(10).items():
                st.write(f"• {make} {model}: {count} purchases")
        with model_col2:
            # Create a bar chart of top 10 models
            top_10_models = all_models.head(10)
            fig = px.bar(
                x=[f"{make} {model}" for make, model in top_10_models.index],
                y=top_10_models.values,
                title='Top 10 Models',
                labels={'x': 'Model', 'y': 'Number of Purchases'}
            )
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)


    @st.dialog("Price Distribution Analysis")
    def show_price_analysis(dealer_historical, price_stats):
        # Create two columns
        price_col1, price_col2 = st.columns(2)
        with price_col1:
            st.write("**Price Statistics:**")
            for stat, value in price_stats.items():
                st.write(f"• {stat}: {value}")
        with price_col2:
            # Create a histogram of prices
            fig = px.histogram(
                dealer_historical,
                x='price',
                title='Price Distribution',
                labels={'price': 'Price (EGP)', 'count': 'Number of Purchases'}
            )
            st.plotly_chart(fig, use_container_width=True)


    def main():
        st.title("🚗 SET - Sales Enablement Tool")

        # Load data
        with st.spinner("Loading data..."):
            historical_df, live_cars_df, dealer_seg_df, activity_df, recent_views_df, recent_filters_df = load_data()

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

                # Add debug section to show all unique segmentation cases
                with st.expander("Debug: All Segmentation Cases"):
                    st.write("**All Unique Segmentation Cases Found:**")
                    all_cases = attention_dealers['current_segmentation'].unique()

                    # Group cases by priority
                    critical_found = []
                    high_found = []
                    medium_found = []
                    low_found = []
                    unmatched = []

                    for case in sorted(all_cases):
                        test_row = pd.Series({'current_segmentation': case})
                        priority = get_priority(test_row)

                        if priority == '🔴 Critical Priority':
                            critical_found.append(case)
                        elif priority == '🟠 High Priority':
                            high_found.append(case)
                        elif priority == '🟡 Medium Priority':
                            medium_found.append(case)
                        elif priority == '⚪ Low Priority':
                            low_found.append(case)
                        else:
                            unmatched.append(case)

                    col1, col2 = st.columns(2)
                    with col1:
                        st.write("🔴 **Critical Priority Cases Found:**")
                        for case in sorted(critical_found):
                            st.write(f"- {case}")

                        st.write("\n🟠 **High Priority Cases Found:**")
                        for case in sorted(high_found):
                            st.write(f"- {case}")

                    with col2:
                        st.write("🟡 **Medium Priority Cases Found:**")
                        for case in sorted(medium_found):
                            st.write(f"- {case}")

                        st.write("\n⚪ **Low Priority Cases Found:**")
                        for case in sorted(low_found):
                            st.write(f"- {case}")

                    if unmatched:
                        st.error("**Unmatched Cases:**")
                        for case in sorted(unmatched):
                            st.write(f"- {case}")

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

            # Add search method selection
            search_method = st.sidebar.radio(
                "Search by",
                ["Dealer Name", "Dealer Code"]
            )

            # Get list of dealers and their codes
            valid_dealers_df = pd.DataFrame({
                'dealer_name': dealer_seg_df['dealer_name'],
                'dealer_code': dealer_seg_df['dealer_code']
            }).drop_duplicates()

            # Check if we came from inbox
            default_dealer = st.query_params.get('dealer', None)

            if search_method == "Dealer Name":
                # Sort dealers by name
                valid_dealers = sorted(valid_dealers_df['dealer_name'].unique())
                default_index = valid_dealers.index(default_dealer) if default_dealer in valid_dealers else 0

                # Dealer selection
                selected_dealer_name = st.sidebar.selectbox(
                    "Select Dealer",
                    options=valid_dealers,
                    index=default_index
                )
                # Get corresponding dealer code
                selected_dealer_code = \
                valid_dealers_df[valid_dealers_df['dealer_name'] == selected_dealer_name]['dealer_code'].iloc[0]
            else:
                # Sort dealers by code
                valid_dealer_codes = sorted(valid_dealers_df['dealer_code'].unique())

                selected_dealer_code = st.sidebar.selectbox(
                    "Select Dealer Code",
                    options=valid_dealer_codes
                )
                # Get corresponding dealer name
                selected_dealer_name = \
                valid_dealers_df[valid_dealers_df['dealer_code'] == selected_dealer_code]['dealer_name'].iloc[0]

            # Display selected dealer info
            st.sidebar.info(f"Selected Dealer: {selected_dealer_name}\nDealer Code: {selected_dealer_code}")

            # Get dealer details using the name (since that's how the dataframes are indexed)
            dealer_info = dealer_seg_df[dealer_seg_df['dealer_name'] == selected_dealer_name]
            dealer_activity = activity_df[activity_df['dealer_name'] == selected_dealer_name]
            dealer_historical = historical_df[historical_df['dealer_name'] == selected_dealer_name].copy()

            if dealer_info.empty:
                st.error(f"No segmentation data found for dealer: {selected_dealer_name}")
                return

            if dealer_activity.empty:
                st.error(f"No activity data found for dealer: {selected_dealer_name}")
                return

            # Get single row data
            dealer_info = dealer_info.iloc[0]
            dealer_activity = dealer_activity.iloc[0]

            # Initialize metrics variables
            top_makes_str = "No data"
            top_models_str = "No data"
            top_mileage_str = "No data"
            price_range = "No data"

            # Get historical purchase patterns
            if not dealer_historical.empty:
                # Calculate top makes
                top_makes = dealer_historical['make'].value_counts().head(3)
                top_makes_str = ", ".join([f"{make} ({count})" for make, count in top_makes.items()])
                all_makes = dealer_historical['make'].value_counts()

                # Calculate top models
                top_models = dealer_historical.groupby(['make', 'model']).size().sort_values(ascending=False).head(3)
                top_models_str = ", ".join([f"{make} {model} ({count})" for (make, model), count in top_models.items()])
                all_models = dealer_historical.groupby(['make', 'model']).size().sort_values(ascending=False)

                # Calculate mileage ranges
                dealer_historical['mileage_range'] = pd.cut(
                    dealer_historical['kilometers'],
                    bins=[0, 50000, 100000, 150000, float('inf')],
                    labels=['0-50K', '50K-100K', '100K-150K', '150K+']
                )
                top_mileage = dealer_historical['mileage_range'].value_counts().head(2)
                top_mileage_str = ", ".join([f"{range_} ({count})" for range_, count in top_mileage.items()])
                all_mileage = dealer_historical['mileage_range'].value_counts()

                # Calculate average price range
                avg_price = dealer_historical['price'].mean()
                price_std = dealer_historical['price'].std()
                price_range = f"EGP {(avg_price - price_std):,.0f} - {(avg_price + price_std):,.0f}"

                # Calculate detailed price statistics
                price_stats = {
                    'Average Price': f"EGP {avg_price:,.0f}",
                    'Minimum Price': f"EGP {dealer_historical['price'].min():,.0f}",
                    'Maximum Price': f"EGP {dealer_historical['price'].max():,.0f}",
                    'Median Price': f"EGP {dealer_historical['price'].median():,.0f}",
                    'Price Range (±1 std)': price_range
                }

            # Key metrics
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("Active Days (30d)", int(dealer_activity['active_days_30d']))
                st.metric("Car Events (30d)", int(dealer_activity['total_car_events_30d']))

            with col2:
                st.metric("Active Days (7d)", int(dealer_activity['active_days_7d']))
                st.metric("Car Events (7d)", int(dealer_activity['total_car_events_7d']))

            with col3:
                st.metric("Top Makes", len(all_makes) if not dealer_historical.empty else "No data")
                if not dealer_historical.empty and st.button("Top Makes Analysis", key="show_makes"):
                    show_makes_analysis(all_makes)

                st.metric("Preferred Mileage", len(all_mileage) if not dealer_historical.empty else "No data")
                if not dealer_historical.empty and st.button("Mileage Analysis", key="show_mileage"):
                    show_mileage_analysis(all_mileage)

            with col4:
                st.metric("Top Models", len(all_models) if not dealer_historical.empty else "No data")
                if not dealer_historical.empty and st.button("Models Analysis", key="show_models"):
                    show_models_analysis(all_models)

                st.metric("Price Analysis", "View" if not dealer_historical.empty else "No data")
                if not dealer_historical.empty and st.button("Price Analysis", key="show_price"):
                    show_price_analysis(dealer_historical, price_stats)

            # Segmentation Information
            st.subheader("Dealer Segmentation")

            seg_col1, seg_col2 = st.columns(2)

            with seg_col1:
                st.info(f"Lifetime Segment: {dealer_info['final_bucket_lifetime']}")
                st.info(f"60-Day Segment: {dealer_info['final_bucket_60d']}")

            with seg_col2:
                st.info(f"Request Activity (Lifetime): {dealer_info['request_activity_bucket_lifetime']}")
                st.info(f"Request Activity (60d): {dealer_info['request_activity_bucket_60d']}")

            # Recent Activity section
            st.subheader("Recent Activity")

            # Get dealer's recent views and filters
            dealer_views = recent_views_df[recent_views_df['dealer_code'] == dealer_info['dealer_code']].copy()
            dealer_filters = recent_filters_df[recent_filters_df['dealer_code'] == dealer_info['dealer_code']].copy()

            # Create tabs for views and filters
            recent_tab1, recent_tab2 = st.tabs(["🔍 Recent Views", "🎯 Recent Filters"])

            with recent_tab1:
                if not dealer_views.empty:
                    # Format the time column
                    dealer_views['time'] = pd.to_datetime(dealer_views['time']).dt.strftime('%Y-%m-%d %H:%M:%S')

                    # Format the price column
                    dealer_views['buy_now_price'] = dealer_views['buy_now_price'].apply(
                        lambda x: f"EGP {x:,.0f}" if pd.notnull(x) else "N/A"
                    )

                    # Format the kilometrage column
                    dealer_views['kilometrage'] = dealer_views['kilometrage'].apply(
                        lambda x: f"{x:,.0f} km" if pd.notnull(x) else "N/A"
                    )

                    # Display recent views
                    st.dataframe(
                        dealer_views[['time', 'make', 'model', 'trim', 'year', 'kilometrage',
                                      'transmission', 'buy_now_price', 'body_style']],
                        column_config={
                            "time": "Viewed At",
                            "make": "Make",
                            "model": "Model",
                            "trim": "Trim",
                            "year": "Year",
                            "kilometrage": "Mileage",
                            "transmission": "Transmission",
                            "buy_now_price": "Price",
                            "body_style": "Body Style"
                        },
                        use_container_width=True
                    )
                else:
                    st.info("No recent car views found for this dealer")

            with recent_tab2:
                if not dealer_filters.empty:
                    # Format the time column
                    dealer_filters['time'] = pd.to_datetime(dealer_filters['time']).dt.strftime('%Y-%m-%d %H:%M:%S')

                    # Format the kilometrage column
                    dealer_filters['kilometrage'] = dealer_filters['kilometrage'].apply(
                        lambda x: f"{x:,.0f} km" if pd.notnull(x) else "N/A"
                    )

                    # Display recent filters
                    st.dataframe(
                        dealer_filters[['time', 'make', 'model', 'year', 'kilometrage',
                                        'group_filter', 'status', 'no_of_cars']],
                        column_config={
                            "time": "Filter Applied At",
                            "make": "Make",
                            "model": "Model",
                            "year": "Year",
                            "kilometrage": "Mileage Range",
                            "group_filter": "Filter Group",
                            "status": "Status",
                            "no_of_cars": st.column_config.NumberColumn(
                                "Number of Cars",
                                help="Number of cars matching the filter criteria"
                            )
                        },
                        use_container_width=True
                    )
                else:
                    st.info("No recent filter applications found for this dealer")

            # Add Dealer Requests section
            st.subheader("Dealer Requests")

            if dealer_info.empty:
                st.warning("No dealer information available")
            else:
                try:
                    # Get credentials and create client
                    try:
                        credentials = service_account.Credentials.from_service_account_info(
                            st.secrets["service_account"]
                        )
                    except (KeyError, FileNotFoundError):
                        try:
                            credentials = service_account.Credentials.from_service_account_file(
                                'service_account.json'
                            )
                        except FileNotFoundError:
                            st.error("No credentials found for BigQuery access")
                            credentials = None

                    if credentials:
                        client = bigquery.Client(credentials=credentials)
                        dealer_data = dealer_seg_df[dealer_seg_df['dealer_name'] == selected_dealer_name].iloc[0]
                        dealer_id = dealer_data['dealer_code']

                        all_requests, succeeded_requests, failed_before_requests, failed_after_requests = get_dealer_requests(
                            client, dealer_id)

                        # Create tabs for different request types
                        req_tab1, req_tab2, req_tab3, req_tab4 = st.tabs([
                            "📋 All Requests",
                            "✅ Succeeded Requests",
                            "❌ Failed Before Visit",
                            "⚠️ Failed After Visit"
                        ])

                        with req_tab1:
                            if not all_requests.empty:
                                st.dataframe(
                                    all_requests,
                                    column_config={
                                        "vehicle_request_created_at": "Request Date",
                                        "request_type": "Request Type",
                                        "request_status": "Status",
                                        "contacted_at": "Contacted At",
                                        "contacted_user": "Contacted By",
                                        "visited_at": "Visit Date",
                                        "visited_user": "Visited By",
                                        "succeeded_at": "Success Date",
                                        "failed_before_visit_at": "Failed Before Visit At",
                                        "failed_after_visit_at": "Failed After Visit At",
                                        "failure_reason": "Failure Reason",
                                        "car_name": "Car Name",
                                        "car_make": "Make",
                                        "car_model": "Model",
                                        "car_year": "Year",
                                        "car_kilometrage": "Mileage",
                                        "buy_now_price": "Buy Now Price",
                                        "discounted_price": "Discounted Price"
                                    },
                                    use_container_width=True
                                )
                            else:
                                st.info("No requests found")

                        with req_tab2:
                            if not succeeded_requests.empty:
                                st.dataframe(
                                    succeeded_requests,
                                    column_config={
                                        "vehicle_request_created_at": "Request Date",
                                        "request_type": "Request Type",
                                        "contacted_at": "Contacted At",
                                        "contacted_user": "Contacted By",
                                        "visited_at": "Visit Date",
                                        "visited_user": "Visited By",
                                        "succeeded_at": "Success Date",
                                        "car_name": "Car Name",
                                        "car_make": "Make",
                                        "car_model": "Model",
                                        "car_year": "Year",
                                        "car_kilometrage": "Mileage",
                                        "buy_now_price": "Buy Now Price",
                                        "discounted_price": "Discounted Price"
                                    },
                                    use_container_width=True
                                )
                            else:
                                st.info("No successful requests found")

                        with req_tab3:
                            if not failed_before_requests.empty:
                                st.dataframe(
                                    failed_before_requests,
                                    column_config={
                                        "vehicle_request_created_at": "Request Date",
                                        "request_type": "Request Type",
                                        "contacted_at": "Contacted At",
                                        "contacted_user": "Contacted By",
                                        "failed_before_visit_at": "Failed At",
                                        "failure_reason": "Failure Reason",
                                        "car_name": "Car Name",
                                        "car_make": "Make",
                                        "car_model": "Model",
                                        "car_year": "Year",
                                        "car_kilometrage": "Mileage",
                                        "buy_now_price": "Buy Now Price",
                                        "discounted_price": "Discounted Price"
                                    },
                                    use_container_width=True
                                )
                            else:
                                st.info("No requests failed before visit")

                        with req_tab4:
                            if not failed_after_requests.empty:
                                st.dataframe(
                                    failed_after_requests,
                                    column_config={
                                        "vehicle_request_created_at": "Request Date",
                                        "request_type": "Request Type",
                                        "contacted_at": "Contacted At",
                                        "contacted_user": "Contacted By",
                                        "visited_at": "Visit Date",
                                        "visited_user": "Visited By",
                                        "failed_after_visit_at": "Failed At",
                                        "failure_reason": "Failure Reason",
                                        "car_name": "Car Name",
                                        "car_make": "Make",
                                        "car_model": "Model",
                                        "car_year": "Year",
                                        "car_kilometrage": "Mileage",
                                        "buy_now_price": "Buy Now Price",
                                        "discounted_price": "Discounted Price"
                                    },
                                    use_container_width=True
                                )
                            else:
                                st.info("No requests failed after visit")
                except Exception as e:
                    st.error(f"Error fetching dealer requests: {str(e)}")

            # Add OLX Listings section
            st.subheader("OLX Listings")

            if dealer_info.empty:
                st.warning("No dealer information available")
            else:
                try:
                    # Get credentials and create client
                    try:
                        credentials = service_account.Credentials.from_service_account_info(
                            st.secrets["service_account"]
                        )
                    except (KeyError, FileNotFoundError):
                        try:
                            credentials = service_account.Credentials.from_service_account_file(
                                'service_account.json'
                            )
                        except FileNotFoundError:
                            st.error("No credentials found for BigQuery access")
                            credentials = None

                    if credentials:
                        client = bigquery.Client(credentials=credentials)
                        dealer_data = dealer_seg_df[dealer_seg_df['dealer_name'] == selected_dealer_name].iloc[0]
                        dealer_id = dealer_data['dealer_code']

                        olx_listings = get_olx_listings_for_dealer(client, dealer_id)

                        if not olx_listings.empty:
                            # Format the dataframe for display
                            display_df = olx_listings[[
                                'added_at', 'title', 'make', 'model', 'year', 'kilometers',
                                'price', 'condition', 'is_active', 'region'
                            ]].copy()

                            # Format date columns
                            display_df['added_at'] = pd.to_datetime(display_df['added_at']).dt.strftime(
                                '%Y-%m-%d %H:%M')

                            # Format price
                            display_df['price'] = display_df['price'].apply(
                                lambda x: f"{x:,.0f} EGP" if pd.notnull(x) else "N/A"
                            )

                            st.dataframe(
                                display_df,
                                column_config={
                                    "added_at": "Listed Date",
                                    "title": "Title",
                                    "make": "Make",
                                    "model": "Model",
                                    "year": "Year",
                                    "kilometers": "Mileage",
                                    "price": "Price",
                                    "condition": "Condition",
                                    "is_active": "Active",
                                    "region": "Region"
                                },
                                use_container_width=True
                            )

                            # Add summary metrics
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                active_listings = len(olx_listings[olx_listings['is_active']])
                                st.metric("Active Listings", active_listings)
                            with col2:
                                avg_price = olx_listings['price'].mean()
                                st.metric("Average Price", f"{avg_price:,.0f} EGP")
                            with col3:
                                unique_models = len(olx_listings[['make', 'model']].drop_duplicates())
                                st.metric("Unique Models", unique_models)
                        else:
                            st.info("No OLX listings found for this dealer in the last 30 days")
                except Exception as e:
                    st.error("Error fetching OLX listings")

            # Get dealer historical data for recommendations and analysis
            dealer_historical = historical_df[historical_df['dealer_name'] == selected_dealer_name].copy()

            # Recommended Cars section
            st.subheader("Recommended Cars")

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

            # Historical Purchase Analysis
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

            else:
                st.warning("No historical purchase data available for this dealer")


    if __name__ == "__main__":
        main()
