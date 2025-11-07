"""
QH Transpose Streamlit App
Batch CSV processor for German quarter-hourly energy meter data
"""


import streamlit as st
import pandas as pd
import os
from datetime import datetime, timedelta
import io
import warnings


# ============================================
# PAGE CONFIGURATION
# ============================================


st.set_page_config(
    page_title="QH Transpose - Energy Data Processor",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================
# CUSTOM CSS FOR BETTER UI
# ============================================


st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        border-left: 4px solid #1f77b4;
    }
    .success-box {
        background-color: #d4edda;
        border-left: 4px solid #28a745;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .warning-box {
        background-color: #fff3cd;
        border-left: 4px solid #ffc107;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
    .error-box {
        background-color: #f8d7da;
        border-left: 4px solid #dc3545;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
    }
</style>
""", unsafe_allow_html=True)


# ============================================
# UTILITY FUNCTIONS
# ============================================


def find_data_start_row(file_content, sep=';'):
    """Dynamically find where data starts by detecting date pattern"""
    encodings = ['utf-8-sig', 'utf-8', 'latin-1', 'cp1252']


    for encoding in encodings:
        try:
            lines = file_content.decode(encoding).split('\n')
            for i, line in enumerate(lines):
                parts = line.split(sep)
                if parts and len(parts[0]) >= 8:
                    try:
                        datetime.strptime(parts[0].split()[0], "%d%m%Y")
                        return i, encoding
                    except (ValueError, IndexError):
                        continue
        except (UnicodeDecodeError, IOError):
            continue


    return 4, 'utf-8-sig'


def find_label_columns(df, max_search=15):
    """Find columns containing energy type (KWT/KVR) and direction (A+/A-/I-/I+/C-/C+)"""
    energy_col = None
    direction_col = None


    for col_idx in range(min(max_search, len(df.columns))):
        sample = df[col_idx].dropna().astype(str).str.strip().str.upper()


        if sample.isin(['KWT', 'KVR']).any():
            energy_col = col_idx


        if sample.isin(['A+', 'A-', 'I+', 'I-', 'C+', 'C-']).any():
            direction_col = col_idx


        if energy_col is not None and direction_col is not None:
            return energy_col, direction_col


    return energy_col, direction_col


def find_value_columns(row, start_search=9, expected_count=96):
    """Find exactly 96 consecutive numeric columns - strict validation"""
    numeric_cols = []


    for col_idx in range(start_search, len(row)):
        if pd.isna(row[col_idx]):
            if len(numeric_cols) == expected_count:
                return numeric_cols
            continue


        val = str(row[col_idx]).replace(',', '.').strip()


        try:
            float(val)
            numeric_cols.append(col_idx)


            if len(numeric_cols) == expected_count:
                return numeric_cols


        except (ValueError, TypeError):
            if len(numeric_cols) == expected_count:
                return numeric_cols
            if len(numeric_cols) > 0:
                break


    return numeric_cols if len(numeric_cols) == expected_count else None


def process_csv_file(file_content, file_name, start_date, end_date, progress_callback=None):
    """Process a single CSV file and return results"""
    errors = []
    warnings_list = []


    # Step 1: Find where data starts
    skip_rows, encoding = find_data_start_row(file_content)


    # Step 2: Read CSV
    try:
        file_stream = io.BytesIO(file_content)
        df = pd.read_csv(
            file_stream,
            sep=';',
            header=None,
            skiprows=skip_rows,
            dtype=str,
            encoding=encoding,
            on_bad_lines='skip'
        )
    except Exception as e:
        return None, [f"Failed to read file: {str(e)}"], warnings_list, None


    if df.empty:
        return None, ["No data found in file"], warnings_list, None


    # Remove mid-file headers
    before_len = len(df)
    df = df[~df[0].astype(str).str.startswith("[")]
    df = df.reset_index(drop=True)
    removed_headers = before_len - len(df)


    # Step 3: Find label columns
    energy_col, direction_col = find_label_columns(df)
    if direction_col is None:
        return None, ["Could not find A+/A- direction column"], warnings_list, None


    label_col = direction_col


    # Step 4: Find value columns
    value_cols = None
    for idx in range(min(10, len(df))):
        value_cols = find_value_columns(df.iloc[idx], start_search=label_col + 1, expected_count=96)
        if value_cols:
            break


    if not value_cols or len(value_cols) != 96:
        return None, [f"Could not find exactly 96 quarter-hourly value columns (found {len(value_cols) if value_cols else 0})"], warnings_list, None


    # Step 5: Process rows
    records = []
    rows_processed = 0
    rows_skipped_label = 0
    rows_skipped_date = 0
    rows_outside_range = 0
    first_date_found = None
    last_date_found = None


    for idx in range(len(df)):
        row = df.iloc[idx]
        rows_processed += 1


        # Check label
        label = str(row[label_col]) if pd.notna(row[label_col]) else ""
        label = label.strip().replace("\xa0", "").upper()
        if label not in {"A-", "A+"}:
            rows_skipped_label += 1
            continue


        # Parse date
        try:
            date_str = str(row[0]).split()[0]
            date_obj = datetime.strptime(date_str, "%d%m%Y")


            if first_date_found is None:
                first_date_found = date_obj
            last_date_found = date_obj
        except Exception as e:
            errors.append(f"Row {idx}: Invalid date format - '{row[0]}'")
            rows_skipped_date += 1
            continue


        # Filter by date range
        if not (start_date <= date_obj <= end_date):
            rows_outside_range += 1
            continue


        date_fmt = date_obj.strftime("%d/%m/%Y")


        # Extract values
        values = [row[col] for col in value_cols]


        # Convert values
        for i, val in enumerate(values):
            time_str = (datetime.strptime("00:00", "%H:%M") + timedelta(minutes=15 * i)).strftime("%H:%M:%S")
            try:
                value_clean = str(val).replace(',', '.').strip()
                value_float = float(value_clean) if value_clean and value_clean != '' else 0.0
                records.append([date_fmt, time_str, value_float])
            except Exception as e:
                warnings_list.append(f"Row {idx}, QH {i+1}: Invalid value '{val}' - using 0.0")
                records.append([date_fmt, time_str, 0.0])


        if progress_callback and idx % 100 == 0:
            progress_callback(idx / len(df))


    # Create summary
    summary = {
        'file_name': file_name,
        'encoding': encoding,
        'removed_headers': removed_headers,
        'label_col': label_col,
        'energy_col': energy_col,
        'value_cols_range': f"{value_cols[0]} to {value_cols[-1]}",
        'rows_processed': rows_processed,
        'rows_skipped_label': rows_skipped_label,
        'rows_skipped_date': rows_skipped_date,
        'rows_outside_range': rows_outside_range,
        'valid_records': len(records),
        'date_range': f"{first_date_found.strftime('%d/%m/%Y')} to {last_date_found.strftime('%d/%m/%Y')}" if first_date_found else "N/A"
    }


    if not records:
        return None, ["No valid A-/A+ rows found in date range"], warnings_list, summary


    # Create output dataframe
    output_df = pd.DataFrame(records, columns=["Timestamp", "Time", "Value [kWh]"])


    return output_df, errors, warnings_list, summary


# ============================================
# MAIN APP
# ============================================


def main():
    # Header
    st.markdown('<p class="main-header">⚡ QH Transpose</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Batch CSV Processor for German Quarter-Hourly Energy Meter Data</p>', unsafe_allow_html=True)


    # Sidebar
    with st.sidebar:
        st.header("📋 Configuration")


        # Date range selection
        st.subheader("📅 Date Range")


        current_year = datetime.now().year
        years = ["All Years"] + [str(year) for year in range(2020, current_year + 2)]


        year_selection = st.selectbox(
            "Quick Select Year",
            years,
            index=years.index(str(current_year)) if str(current_year) in years else 0
        )


        if year_selection == "All Years":
            start_date = datetime(2020, 1, 1)
            end_date = datetime(2030, 12, 31)
        else:
            start_date = datetime(int(year_selection), 1, 1)
            end_date = datetime(int(year_selection), 12, 31)


        st.write(f"**Date Range:** {start_date.strftime('%d/%m/%Y')} to {end_date.strftime('%d/%m/%Y')}")


        # Custom date range
        with st.expander("🔧 Custom Date Range", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                custom_start = st.date_input("Start Date", start_date)
            with col2:
                custom_end = st.date_input("End Date", end_date)


            if st.button("Apply Custom Range"):
                start_date = datetime.combine(custom_start, datetime.min.time())
                end_date = datetime.combine(custom_end, datetime.min.time())
                st.success("✅ Custom range applied!")


        st.markdown("---")


        # Instructions
        st.subheader("ℹ️ How to Use")
        st.markdown("""
        1. **Upload** one or more CSV files
        2. **Select** date range (or use custom)
        3. **Click** "Process Files"
        4. **Download** cleaned Excel files
        """)


        st.markdown("---")


        st.subheader("📊 Features")
        st.markdown("""
        ✅ Auto-detects file encoding
        ✅ Handles mid-file headers
        ✅ Filters by A-/A+ labels
        ✅ Extracts 96 QH values
        ✅ Date range filtering
        ✅ Batch processing
        """)


    # Main content
    st.header("📤 Upload Files")


    uploaded_files = st.file_uploader(
        "Choose CSV files to process",
        type=['csv'],
        accept_multiple_files=True,
        help="Select one or more CSV files containing quarter-hourly energy meter data"
    )


    if uploaded_files:
        st.success(f"✅ {len(uploaded_files)} file(s) uploaded successfully!")


        # Display file info
        with st.expander("📋 Uploaded Files", expanded=True):
            for i, file in enumerate(uploaded_files, 1):
                st.write(f"{i}. **{file.name}** ({file.size / 1024:.1f} KB)")


        st.markdown("---")


        # Process button
        if st.button("🚀 Process Files", type="primary", use_container_width=True):
            st.header("⚙️ Processing")


            overall_progress = st.progress(0)
            status_text = st.empty()


            results = []


            for idx, uploaded_file in enumerate(uploaded_files):
                status_text.text(f"Processing {idx + 1}/{len(uploaded_files)}: {uploaded_file.name}")


                # Read file content
                file_content = uploaded_file.read()
                uploaded_file.seek(0)  # Reset for potential re-reading


                # Process file
                with st.expander(f"📄 {uploaded_file.name}", expanded=True):
                    file_progress = st.progress(0)


                    def update_progress(value):
                        file_progress.progress(value)


                    output_df, errors, warnings_list, summary = process_csv_file(
                        file_content,
                        uploaded_file.name,
                        start_date,
                        end_date,
                        progress_callback=update_progress
                    )


                    if output_df is not None:
                        # Success
                        st.markdown('<div class="success-box">✅ <strong>Processing completed successfully!</strong></div>', unsafe_allow_html=True)


                        # Display summary
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            st.metric("Valid Records", f"{summary['valid_records']:,}")
                        with col2:
                            st.metric("Rows Processed", summary['rows_processed'])
                        with col3:
                            st.metric("Skipped (Label)", summary['rows_skipped_label'])
                        with col4:
                            st.metric("Outside Range", summary['rows_outside_range'])


                        st.write(f"**Date Range in File:** {summary['date_range']}")
                        st.write(f"**Encoding:** {summary['encoding']}")


                        if summary['removed_headers'] > 0:
                            st.info(f"ℹ️ Removed {summary['removed_headers']} mid-file header rows")


                        # Warnings
                        if warnings_list:
                            with st.expander(f"⚠️ {len(warnings_list)} Warnings", expanded=False):
                                for warning in warnings_list[:10]:  # Show first 10
                                    st.warning(warning)
                                if len(warnings_list) > 10:
                                    st.write(f"... and {len(warnings_list) - 10} more warnings")


                        # Preview data
                        with st.expander("👀 Preview Data", expanded=False):
                            st.dataframe(output_df.head(100), use_container_width=True)


                        # Download button
                        output_buffer = io.BytesIO()
                        with pd.ExcelWriter(output_buffer, engine='openpyxl') as writer:
                            output_df.to_excel(writer, index=False, sheet_name='Data')


                        st.download_button(
                            label="📥 Download Excel File",
                            data=output_buffer.getvalue(),
                            file_name=f"{os.path.splitext(uploaded_file.name)[0]}_cleaned.xlsx",
                            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                            use_container_width=True
                        )


                        results.append({'file': uploaded_file.name, 'status': 'success', 'records': summary['valid_records']})


                    else:
                        # Error
                        st.markdown('<div class="error-box">❌ <strong>Processing failed!</strong></div>', unsafe_allow_html=True)


                        for error in errors:
                            st.error(error)


                        if summary:
                            st.write("**Partial Summary:**")
                            st.json(summary)


                        results.append({'file': uploaded_file.name, 'status': 'failed', 'records': 0})


                overall_progress.progress((idx + 1) / len(uploaded_files))


            status_text.text("✅ All files processed!")


            # Summary
            st.markdown("---")
            st.header("📊 Batch Processing Summary")


            success_count = sum(1 for r in results if r['status'] == 'success')
            failed_count = len(results) - success_count
            total_records = sum(r['records'] for r in results)


            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("✅ Successful", success_count)
            with col2:
                st.metric("❌ Failed", failed_count)
            with col3:
                st.metric("📝 Total Records", f"{total_records:,}")


            # Results table
            st.subheader("📋 Detailed Results")
            results_df = pd.DataFrame(results)
            st.dataframe(results_df, use_container_width=True)


if __name__ == "__main__":
    main()



