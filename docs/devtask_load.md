# Data Loading Development Plan

## Current Structure

The data loading portion of the Trendeez pipeline is organized into four distinct groups:

1. **ingest_initial** (One-Time Execution)
   - `initial_load.py` - Historical Billboard chart data from 2005 onwards
   - `initial_lyrics.py` - Historical lyrics data from large lyrics dataset with Genius API fallback

2. **ingest_gap** (One-Time Execution)
   - `gap_load.py` - Billboard data between historical and current periods
   - Ensures data continuity by filling gaps

3. **ingest_current** (Weekly Schedule)
   - `current_load.py` - Weekly Billboard Hot 100 chart updates
   - Identifies new songs needing lyrics

4. **lyrics_fetching** (Weekly Schedule)
   - `lyrics_fetching.py` - Consolidated lyrics fetching for all sources
   - Single point of interaction with Genius API

## Data Sources

1. **Historical Billboard charts**
   - Source: [RWD Billboard Data Repository](https://github.com/utdata/rwd-billboard-data/tree/main/data-out)
   - File: `hot-100-initial.csv`
   - Included in repository

2. **Historical lyrics**
   - Source: [Genius Song Lyrics Dataset](https://www.kaggle.com/datasets/carlosgdcj/genius-song-lyrics-with-language-information)
   - File: `song_lyrics.csv` (~9GB)
   - Not included in repository (size constraints)
   - Users must download separately

3. **Current Billboard charts**
   - Source: billboard.py library
   - Real-time scraping of Billboard website

4. **Missing lyrics**
   - Source: Genius API
   - Centralized fetching for all sources

## Implementation Plan

### Phase 1: Initial Data Loading

#### Task 1: Initial Billboard Chart Data
- [x] Create asset to load historical Billboard Hot 100 data from CSV
- [x] Configure BigQuery table creation and data loading
- [x] Implement error handling and logging
- [x] Add filter for data from 2005 onwards

#### Task 2: Initial Lyrics Data
- [x] Create asset to load historical lyrics data from large dataset
  - [x] Parse and clean the CSV file
  - [x] Define schema mapping
  - [x] Load data to BigQuery raw.historical_lyrics table
  - [x] Filter out misc tags and pre-2000 songs
- [x] Create asset to identify songs missing lyrics in the historical data
  - [x] Join Billboard historical data with loaded lyrics data
  - [x] Generate list of songs that need lyrics from Genius

### Phase 2: Gap Data Loading

#### Task 3: Historical Gap Coverage
- [x] Create asset to identify date range gap
- [x] Implement Billboard data fetching for gap period
- [x] Store gap data in BigQuery for direct access by consolidated lyrics process
- [x] Ensure empty tables are created when no gap exists

### Phase 3: Weekly Update Process

#### Task 4: Current Billboard Chart Updates
- [x] Create asset to fetch current Billboard Hot 100 data
- [x] Implement BigQuery loading with appropriate schema
- [x] Add error handling and logging
- [x] Generate new songs list for lyrics fetching

#### Task 5: Consolidated Lyrics Fetching
- [x] Create distinct_list_of_songs asset
  - [x] Combine sources (historical, gap, current) directly from source tables
  - [x] Track source and priority
  - [x] Handle deduplication
- [x] Implement unified fetch_lyrics asset
  - [x] Use lyricsgenius library
  - [x] Implement priority-based processing
  - [x] Add comprehensive status tracking

### Phase 4: Resilient Lyrics Fetching

#### Task 9: Lyrics Fetching Resilience
- [x] Ensure all_song_lyrics table exists before processing
  - [x] Create table schema if not exists function
  - [x] Initialize empty table on first run
- [x] Update distinct_list_of_songs asset
  - [x] Exclude songs that already have successful lyrics
  - [x] Handle case when table doesn't exist
  - [x] Remove redundant intermediate tables
  - [x] Add processing_run_id field for tracking
- [x] Implement batch processing in fetch_lyrics
  - [x] Process songs in smaller batches (50-100 songs)
  - [x] Save results after each batch completion
  - [x] Add batch progress reporting
  - [x] Include processing_run_id in saved data
- [x] Handle authentication challenges
  - [x] Use service account key file for authentication
  - [x] Implement proper error handling for auth issues

The resilient approach ensures:
- No loss of progress during long-running operations
- Ability to resume after failures
- Clear tracking of processing status
- No duplicate processing of songs
- Authentication stability for long processes
- Direct data access without unnecessary intermediate tables

### Phase 5: Optimization and Testing

#### Task 6: Performance Optimization
- [x] Implement chunking for large dataset loading
- [x] Add progress tracking for long-running processes
- [ ] Optimize BigQuery operations

#### Task 7: Error Handling Improvements
- [ ] Implement partial success handling
- [ ] Create notification system for failed fetches
- [ ] Add automatic retries for transient errors

#### Task 8: Monitoring and Logging
- [x] Enhance logging to track data lineage
- [ ] Add metrics for load times, success rates
- [ ] Create summary logs for each run

## Testing Strategy

1. **Unit Tests**:
   - Test data cleaning and transformation functions
   - Test API interaction components with mock responses
   - Test artist name matching logic

2. **Integration Tests**:
   - Test end-to-end loading process with sample data
   - Verify BigQuery table creation and data insertion
   - Test consolidated lyrics fetching process

3. **Performance Tests**:
   - Test with full historical dataset
   - Measure and optimize memory usage
   - Verify Genius API rate limit handling

4. **Resilience Tests**:
   - Test recovery from simulated failures
   - Verify batch processing completes correctly
   - Confirm no duplication of work after restarts

## Staging Approach

1. Create staging dataset in BigQuery for temporary storage
2. Implement validation checks before final table insertion
3. Use BigQuery merge operations for updates and deduplication

## Deliverables

- Completed assets for all data loading operations
- BigQuery tables with properly loaded data
- Documentation for each asset and table schema
- Test results demonstrating reliability and performance
- Setup instructions for external data sources
