# --- Dependencies ---
# Ensure these packages are installed: install.packages(c("rvest", "dplyr", "googlesheets4", "purrr", "httr", "lubridate", "readr"))
pacman::p_load(rvest, dplyr, googlesheets4, purrr, httr, lubridate, readr)

# --- Configuration ---
# Set these environment variables before running the script:
# GOOGLE_SHEET_ID: The ID of your Google Sheet (from the URL)
# CONTACT_EMAIL: Your email address (used politely in the User-Agent header)
# Optional for non-interactive auth:
# GOOGLE_SERVICE_ACCOUNT_KEY_PATH: Path to a Google service account JSON key file

ss_id <- Sys.getenv("GOOGLE_SHEET_ID")
contact_email <- Sys.getenv("CONTACT_EMAIL")
# service_account_key <- Sys.getenv("GOOGLE_SERVICE_ACCOUNT_KEY_PATH") # Uncomment if using service account

if (ss_id == "") {
  stop("FATAL: GOOGLE_SHEET_ID environment variable not set.")
}
if (contact_email == "") {
  warning("WARN: CONTACT_EMAIL environment variable not set. User-Agent will be less informative.")
  ua_string <- "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36 (R Scraper Bot)"
} else {
  ua_string <- sprintf("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36 (R Scraper Bot; +mailto:%s)", contact_email)
}


# --- Google Sheets Authentication ---
# Uses cached credentials by default, prompts if needed interactively.
# For non-interactive use (cron, GitHub Actions), cache credentials beforehand
# or use a service account key: gs4_auth(path = service_account_key)
# gs4_auth(path = service_account_key) # Uncomment and use if using service account key
gs4_auth() # Uses cached token or prompts interactively


# --- Functions ---

# Function to parse a single listing
parse_listing <- function(l, beds) {
  # Selectors based on previous findings - verify if Kijiji changes structure
  title_node <- html_nodes(l, '[data-testid="listing-title"]')
  price_node <- html_nodes(l, '[data-testid="listing-price"]')
  desc_node <- html_nodes(l, '[data-testid|="listing-description"]')
  link_node <- html_nodes(l, '[data-testid|="listing-link"]')
  
  # Extract data
  title <- if(length(title_node) > 0) html_text2(title_node) else NA_character_
  price <- if(length(price_node) > 0) html_text(price_node) else NA_character_
  description <- if(length(desc_node) > 0) html_text2(desc_node) else NA_character_
  link_raw <- if(length(link_node) > 0) html_attr(link_node, "href") else NA_character_
  
  # Construct full link
  link <- if(!is.na(link_raw) && !startsWith(link_raw, "http")) {
    if (startsWith(link_raw, "/")) {
      sprintf("https://www.kijiji.ca%s", link_raw)
    } else {
      link_raw
    }
  } else if (!is.na(link_raw)) {
    link_raw
  } else {
    NA_character_
  }
  date <- Sys.Date()
  
  list(
    title = title,
    price = price,
    description = description,
    link = link,
    date = date,
    beds = beds
  )
}

# Function to scrape and triage listings
scrape_and_triage_listings <- function(url, sheet, beds, session) {
  page <- tryCatch({
    rvest::session_jump_to(session, url)
  }, error = function(e) {
    # Keep essential error messages
    message(sprintf("ERROR: Failed fetching URL %s: %s", url, e$message))
    return(NULL)
  })
  
  if (is.null(page)) {
    return(tibble::tibble()) # Return empty on fetch error
  }
  
  # Selector for individual listing cards
  listing_elements <- page %>%
    html_nodes('[data-testid="listing-card"]')
  
  if (length(listing_elements) == 0) {
    # Keep essential failure message
    message(sprintf("INFO: No listing elements found on page %s using selector: [data-testid=\"listing-card\"]", url))
    return(tibble::tibble())
  }
  
  # Parse listings
  listings <- purrr::map_dfr(listing_elements, ~parse_listing(.x, beds))
  
  # Proceed only if listings were parsed
  if (nrow(listings) > 0) {
    listings_valid <- listings %>% dplyr::filter(!is.na(link), !is.na(price), price != "")
    if(nrow(listings_valid) > 0) {
      triage_listings(listings_valid, sheet = sheet) # Pass valid listings
    } else {
      # Optional: Log if parsing yielded no valid data
      # message(sprintf("INFO: Parsing found elements but yielded no valid data for URL: %s", url))
      return(tibble::tibble())
    }
  } else {
    # Optional: Log if parsing failed completely
    # message(sprintf("INFO: Parsing found 0 listings from elements on URL: %s", url))
    return(tibble::tibble())
  }
}


# Function to triage listings
triage_listings <- function(listings, sheet) {
  listings_in <- tryCatch({
    # Specify column types for robustness, adjust "cDl" etc. as needed
    # c=char, D=Date, i=int, d=double, l=logical
    googlesheets4::range_read(ss_id, sheet = sheet, col_types = "cccDci")
  }, error = function(e){
    message(sprintf("ERROR: Reading Google Sheet '%s': %s. Assuming empty.", sheet, e$message))
    # Return an empty tibble with expected columns if sheet/read fails
    tibble::tibble(title=character(), price=character(), description=character(), link=character(), date=lubridate::Date(), beds=integer())
  })
  
  # Ensure listings_in has the 'link' column
  if (!"link" %in% names(listings_in)) {
    listings_in$link <- character(0)
  } else {
    # Ensure link column is character type for the filter
    listings_in$link <- as.character(listings_in$link)
  }
  # Ensure incoming listings link column is character
  listings$link <- as.character(listings$link)
  
  
  listings_out <- listings %>%
    # Ensure consistent data types before filtering/binding
    mutate(
      date = as.Date(date),
      beds = as.integer(beds)
    ) %>%
    filter(
      !is.na(link),
      link != "",
      !link %in% listings_in$link, # Filter out existing links
      !is.na(price),
      price != ""
    )
  
  if (nrow(listings_out) > 0) {
    # Update main sheet
    start_row <- nrow(listings_in) + 2
    start_range <- sprintf("A%d", start_row)
    tryCatch({
      range_write(ss_id,
                  sheet = sheet,
                  data = listings_out,
                  range = start_range,
                  col_names = FALSE,
                  reformat = FALSE)
    }, error = function(e){
      message(sprintf("ERROR: Writing listings to sheet '%s': %s", sheet, e$message))
    })
    
    # Update "New Listings" sheet (clear and write)
    tryCatch({
      range_to_clear = "A1:F1000" # Adjust columns (F) and rows (1000) as needed
      range_clear(ss_id, sheet = "New Listings", range = range_to_clear)
      range_write(ss_id, sheet = "New Listings", data = listings_out, range="A1", col_names=TRUE, reformat = FALSE)
    }, error = function(e){
      message(sprintf("ERROR: Updating 'New Listings' sheet: %s", e$message))
    })
    
  } # No message needed if no new listings
  
  listings_out # Return the newly found listings
}

# --- Main Script ---

# Define URLs (These are public and fine to keep)
urls <- list(
  west_1_bedroom = "https://www.kijiji.ca/b-apartments-condos/city-of-toronto/1+bedroom__bachelor+studio__1+bedroom+den/c37l1700273a27949001?sort=dateDesc&radius=5.0&address=Bloor+St+West+at+Symington+Ave%2C+Toronto%2C+ON+M6P+4H6%2C+Canada&ll=43.657391%2C-79.447578",
  west_2_bedroom = "https://www.kijiji.ca/b-apartments-condos/city-of-toronto/2+bedrooms__2+bedroom+den/c37l1700273a27949001?sort=dateDesc&radius=5.0&address=Bloor+St+West+at+Symington+Ave%2C+Toronto%2C+ON+M6P+4H6%2C+Canada&ll=43.657391%2C-79.447578",
  west_3_bedroom = "https://www.kijiji.ca/b-apartments-condos/city-of-toronto/3+bedrooms__3+bedroom+den__4+bedrooms__4+bedroom+den__5+bedrooms/c37l1700273a27949001?sort=dateDesc&radius=5.0&address=Bloor+St+West+at+Symington+Ave%2C+Toronto%2C+ON+M6P+4H6%2C+Canada&ll=43.657391%2C-79.447578",
  east_1_bedroom = "https://www.kijiji.ca/b-apartments-condos/city-of-toronto/1+bedroom__bachelor+studio__1+bedroom+den/c37l1700273a27949001?sort=dateDesc&radius=6.0&address=Danforth+Ave+at+Coxwell+Ave%2C+Toronto%2C+ON%2C+Canada&ll=43.6833855%2C-79.323606",
  east_2_bedroom = "https://www.kijiji.ca/b-apartments-condos/city-of-toronto/2+bedrooms__2+bedroom+den/c37l1700273a27949001?sort=dateDesc&radius=6.0&address=Danforth+Ave+at+Coxwell+Ave%2C+Toronto%2C+ON%2C+Canada&ll=43.6833855%2C-79.323606",
  east_3_bedroom = "https://www.kijiji.ca/b-apartments-condos/city-of-toronto/3+bedrooms__3+bedroom+den__4+bedrooms__5+bedrooms__4+bedroom+den/c37l1700273a27949001?sort=dateDesc&radius=6.0&address=Danforth+Ave+at+Coxwell+Ave%2C+Toronto%2C+ON%2C+Canada&ll=43.6833855%2C-79.323606"
)

# Define Delay (Keep the delay, just don't announce it)
scrape_delay_seconds <- 7 # Adjust as needed (5-15 seconds typical)

# Create User-Agent Session
ua <- user_agent(ua_string)
kijiji_session <- session("https://www.kijiji.ca", ua)

# --- Main Loop ---
all_new_listings <- list() # Optional: Collect results

for (location in c("west", "east")) {
  for (bedroom_count in c(1, 2, 3)) {
    url_key <- paste(location, bedroom_count, "bedroom", sep = "_")
    sheet_name <- paste("listings -", location, "side", sep = " ") # Use sheet_name consistently
    current_url <- urls[[url_key]]
    
    # Scrape and triage, passing the session
    new_listings_for_url <- tryCatch({
      scrape_and_triage_listings(current_url, sheet = sheet_name, beds = bedroom_count, session = kijiji_session)
    }, error = function(e){
      message(sprintf("FATAL ERROR during scrape/triage for %s: %s", url_key, e$message))
      return(tibble::tibble()) # Return empty tibble on unhandled error
    })
    
    # Optional: Store results
    all_new_listings[[url_key]] <- new_listings_for_url
    
    # --- PAUSE between requests (Essential for not getting blocked) ---
    Sys.sleep(scrape_delay_seconds)
  }
}

# --- Script End ---
# Optional: Add a final completion message if desired
# message("Script finished.")

