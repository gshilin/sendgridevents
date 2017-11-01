package main

import (
	"database/sql"
	_ "github.com/jmoiron/sqlx"
	"fmt"
	"github.com/gin-gonic/gin"
	_ "github.com/gin-contrib/cors"
	_ "github.com/mattn/go-isatty"
	_ "github.com/joho/godotenv/autoload"
	_ "github.com/lib/pq"
	"github.com/MakeNowJust/heredoc"
	"github.com/yvasiyarov/gorelic"
	"log"
	"net/http"
	"os"
	"time"
	"strconv"
	"github.com/jmoiron/sqlx"
	"strings"
)

type Event struct {
	ID        uint
	CreatedAt time.Time
	UpdatedAt time.Time

	Event       string
	Email       string
	Category    string `json:"category"`
	Timestamp   int64
	Happened_at time.Time
	Url         string
	UniqId      string `json:"uniq_id"`
	SmtpId      string `json:"smtp-id"`
	SgMessageId string `json:"sg_message_id"`
	IP          string `json:"ip"`
	UserAgent   string `json:"useragent"`
}

type Events []Event

type SearchSuggestion struct {
	Field string `db:"field"`
}
type SearchSuggestions []SearchSuggestion

var (
	db      *sql.DB
	dbx     *sqlx.DB
	err     interface{}
	eventDB chan (Event)
	quitDB  chan (int)
)

const numOfUpdates = 20

func main() {
	// prepare NewRelic
	configureNewRelic()

	// prepare DB
	db, err = prepareDB()
	if err != nil {
		return
	}

	defer closeDB(db, dbx)

	eventDB = make(chan Event)
	quitDB = make(chan int)
	go updateDb()

	r := gin.Default()
	r.Use(CORSMiddleware())

	r.POST("/api/sendgrid_event", processEvent)
	r.GET("/search/search_suggestions", processSearchSuggestion)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fmt.Println("SERVING on port", port)
	http.ListenAndServe(":"+port, r)
}

func CORSMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
		c.Writer.Header().Set("Access-Control-Max-Age", "86400")
		c.Writer.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE, UPDATE")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "Origin, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")
		c.Writer.Header().Set("Access-Control-Expose-Headers", "Content-Length")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")

		if c.Request.Method == "OPTIONS" {
			fmt.Println("OPTIONS")
			c.AbortWithStatus(200)
		} else {
			c.Next()
		}
	}
}

func prepareDB() (db *sql.DB, err error) {
	db, err = sql.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("DB connection error: %v\n", err)
		return
	}
	err = db.Ping() // really connect to db
	if err != nil {
		log.Fatalf("DB real connection error: %v\n", err)
		return
	}
	dbx, err = sqlx.Open("postgres", os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatalf("DB connection error: %v\n", err)
		return
	}
	err = dbx.Ping() // really connect to db
	if err != nil {
		log.Fatalf("DB real connection error: %v\n", err)
		return
	}

	db.SetMaxOpenConns(numOfUpdates)
	db.SetMaxIdleConns(numOfUpdates)
	dbx.SetMaxOpenConns(numOfUpdates)
	dbx.SetMaxIdleConns(numOfUpdates)

	return
}

func closeDB(db *sql.DB, dbx *sqlx.DB) {
	quitDB <- 0
	db.Close()
	dbx.Close()
}

func processEvent(c *gin.Context) {
	events := Events{}
	if err := c.ShouldBindJSON(&events); err != nil {
		fmt.Println("marshal error:", err)
		return
	}

	for _, event := range events {
		eventDB <- event
	}
}

func processSearchSuggestion(c *gin.Context) {
	c.Writer.Header().Set("Content-Type", "application/json")

	query := c.DefaultQuery("term", "")
	if query == "" {
		c.String(http.StatusOK, "[]")
		return
	}

	couponsTotal, couponIds := ProductSearchIds(query, 3, false)
	vacationsTotal, vacationIds := ProductSearchIds(query, 3, true)
	productsTotal, productIds := ShopProductSearchIds(query, 6)

	var suggestions []string

	if couponsTotal > 0 {
		for _, x := range filterCoupons(couponIds) {
			suggestions = append(suggestions, x.Field)
		}
	}
	if vacationsTotal > 0 {
		for _, x := range filterCoupons(vacationIds) {
			suggestions = append(suggestions, x.Field)
		}
	}
	if productsTotal > 0 {
		for _, x := range filterShopProducts(productIds) {
			suggestions = append(suggestions, x.Field)
		}
	}

	c.String(http.StatusOK, "[%s]", strings.Join(suggestions, ","))
}

func getAO(prefix string) (result string) {
	type AOResult struct {
		Id       int    `db:"id"`
		Ancestry string `db:"ancestry"`
	}
	type AOIds struct {
		Id int `db:"id"`
	}
	var (
		adultsOnlyCategory    AOResult
		adultsOnlySubCatIds   []int = []int{}
		adultsOnlyCatIds      []AOIds
		productsOnlySubCatIds []int
		productSubCatIds      []AOIds
	)
	if err = dbx.Get(&adultsOnlyCategory, `SELECT "categories".id, "categories".ancestry FROM "categories" WHERE "categories".system_name = 'adults-only' LIMIT 1`); err != nil {
		return
	}
	request := heredoc.Docf(`
		SELECT "categories".id
		FROM "categories"
		WHERE (("categories"."ancestry" ILIKE '%s/%d/%%' OR "categories"."ancestry" = '%s/%d') OR "categories"."id" = %d) AND "categories"."is_active" = true
	`, adultsOnlyCategory.Ancestry, adultsOnlyCategory.Id, adultsOnlyCategory.Ancestry, adultsOnlyCategory.Id, adultsOnlyCategory.Id)
	if err = dbx.Select(&adultsOnlyCatIds, request); err != nil {
		return
	}
	for _, a := range adultsOnlyCatIds {
		adultsOnlySubCatIds = append(adultsOnlySubCatIds, a.Id)
	}
	request = heredoc.Docf(`
		SELECT "sub_categories".id
		FROM "sub_categories"
		INNER JOIN "categories_sub_categories" ON "sub_categories"."id" = "categories_sub_categories"."sub_category_id"
		WHERE "categories_sub_categories"."category_id" IN (%s)  ORDER BY categories_sub_categories.priority
	`, arrayToString(adultsOnlySubCatIds, ","))
	if err = dbx.Select(&adultsOnlyCatIds, request); err != nil {
		return
	}
	for _, a := range adultsOnlyCatIds {
		productsOnlySubCatIds = append(productsOnlySubCatIds, a.Id)
	}
	request = heredoc.Docf(`
		SELECT "%sproducts_sub_categories"."product_id" id
		FROM "%sproducts_sub_categories"
		WHERE "%sproducts_sub_categories"."sub_category_id" IN (%s)
	`, prefix, prefix, prefix, arrayToString(productsOnlySubCatIds, ","))
	if err = dbx.Select(&productSubCatIds, request); err != nil && err != sql.ErrNoRows {
		return
	}
	if len(productSubCatIds) > 0 {
		var pairs []int
		for _, x := range productSubCatIds {
			pairs = append(pairs, x.Id)
		}
		return heredoc.Docf(`
			"%sproducts"."id" NOT IN (%s)
		`, prefix, arrayToString(pairs, ","))
	} else {
		return "1=1"
	}
}

func filterShopProducts(ids []int) (result SearchSuggestions) {
	var (
		adults string = getAO("shop_")
		order  []string
	)

	for i, p := range ids {
		order = append(order, fmt.Sprintf("(%d,%d)", p, i))
	}
	request := heredoc.Docf(`
		SELECT '{"href":"/shop/sales/' || "shop_products".sale_id || '/products/' || "shop_products"."id" || '","label":"' || "shop_products"."title" || '"}' field
		FROM "shop_products"
		JOIN (values %s) AS x(id, ordering) ON "shop_products".id = x.id
		WHERE "shop_products"."id" IN (%s) AND (%s)
		ORDER BY x.ordering
	`, strings.Join(order, ","), arrayToString(ids, ","), adults)

	if err = dbx.Select(&result, request); err != nil && err != sql.ErrNoRows {
		result = SearchSuggestions{}
		return
	}

	return
}

func filterCoupons(ids []int) (result SearchSuggestions) {
	var (
		adults string = getAO("")
		order  []string
	)

	for i, p := range ids {
		order = append(order, fmt.Sprintf("(%d,%d)", p, i))
	}
	request := heredoc.Docf(`
		SELECT '{"href":"/products/' || "products"."system_name" || '","label":"' || "products"."title" || '"}' field
		FROM "products"
		JOIN (values %s) AS x(id, ordering) ON "products".id = x.id
		WHERE "products"."id" IN (%s) AND (%s)
		ORDER BY x.ordering
	`, strings.Join(order, ","), arrayToString(ids, ","), adults)

	if err = dbx.Select(&result, request); err != nil && err != sql.ErrNoRows {
		result = SearchSuggestions{}
		return
	}

	return
}

func ProductSearchIds(unsanitizedTerm string, limit int, vacation bool) (total int, ids []int) {
	var request string
	v := "f"
	if vacation {
		v = "t"
	}
	limitQ := ""
	if limit > 0 {
		limitQ = fmt.Sprintf(" LIMIT %d", limit)
	}

	if len(unsanitizedTerm) == 0 {
		request = heredoc.Docf(`
			WITH coupons AS (
				SELECT DISTINCT *
				FROM products
				WHERE ready = 't' AND visible = 't' AND LOCALTIMESTAMP BETWEEN valid_from AND valid_until
				AND vacation = '%s'
			)
			SELECT id, (SELECT COUNT(1) FROM coupons) AS total
			FROM coupons
			ORDER BY id DESC
			%s
		`, v, limitQ)
	} else {
		term := sanitize(unsanitizedTerm)
		request = heredoc.Docf(`
	      WITH coupons AS (
			SELECT DISTINCT *, ((ts_rank(("products"."tsv"), (to_tsquery('simple', ''' ' || '%s' || ' ''' || ':*')), 0))) AS pg_search_rank
			FROM products
			WHERE ready = 't' AND visible = 't' AND LOCALTIMESTAMP BETWEEN valid_from AND valid_until
				AND vacation = '%s'
				AND (
				  tsv @@ to_tsquery('simple', ''' ' || '%s' || ' ''' || ':*') OR
				  tsv @@ to_tsquery('simple', ''' ' || reverse('%s') || ' ''' || ':*')
				)
		  )
		  SELECT id, (SELECT COUNT(1) FROM coupons) AS total, pg_search_rank
		  FROM coupons
		  ORDER BY pg_search_rank DESC
		  %s
		`, term, v, term, term, limitQ)
	}

	type Results struct {
		Id           string `db:"id"`
		Total        string `db:"total"`
		PgSearchRank string `db:"pg_search_rank"`
	}
	results := []Results{}
	if err = dbx.Select(&results, request); err != nil {
		return
	}
	if len(results) == 0 {
		return
	}
	total, _ = strconv.Atoi(results[0].Total)
	for _, r := range results {
		id, _ := strconv.Atoi(r.Id)
		ids = append(ids, id)
	}
	return
}

func ShopProductSearchIds(unsanitizedTerm string, limit int) (total int, ids []int) {
	var request string
	limitQ := ""
	if limit > 0 {
		limitQ = fmt.Sprintf(" LIMIT %d", limit)
	}

	query := sanitize(unsanitizedTerm)
	var finalSaleIds []int = []int{}

	request = heredoc.Docf(`
		 SELECT "shop_products"."id"
		 FROM "shop_products"
		 INNER JOIN "shop_products_sub_categories" ON "shop_products"."id" = "shop_products_sub_categories"."product_id"
		 WHERE "shop_products_sub_categories"."sub_category_id" IN
			 (SELECT  "sub_categories".id FROM "sub_categories" WHERE "sub_categories"."system_name" = 'final-sale')
		`)
	if err = dbx.Select(&finalSaleIds, request); err != nil {
		return
	}
	if len(finalSaleIds) == 0 {
		finalSaleIds = []int{0}
	}

	request = heredoc.Docf(`
      WITH search_products AS (
        SELECT shop_products.id, shop_catalogs.quantity_bought,
          ts_rank("shop_products"."tsv", (to_tsquery('simple', ''' ' || '%s' || ' ''' || ':*')), 0) +
          ts_rank("shop_products"."tsv", (to_tsquery('simple', ''' ' || reverse('%s') || ' ''' || ':*')), 0) AS pg_search_rank
        FROM shop_products
        INNER JOIN shop_option_types ON shop_option_types.product_id = shop_products.id
        INNER JOIN shop_catalogs ON shop_catalogs.option_type_id = shop_option_types.id
        JOIN shop_sales ON shop_products.sale_id = shop_sales.id
        WHERE (shop_sales.hidden = 'f' OR shop_products.id IN (%s)) AND shop_sales.status = 'READY' AND
		  LOCALTIMESTAMP BETWEEN shop_sales.start_date AND shop_sales.end_date AND shop_products.delivery_product = false
          AND (
            tsv @@ to_tsquery('simple', ''' ' || '%s' || ' ''' || ':*') OR
            tsv @@ to_tsquery('simple', ''' ' || reverse('%s') || ' ''' || ':*')
          )
      ),
      grouped AS ( SELECT id, SUM(quantity_bought) AS quantity_bought, pg_search_rank FROM search_products GROUP BY id, pg_search_rank)
      SELECT id, quantity_bought, (SELECT COUNT(1) FROM grouped) AS total
      FROM grouped
      ORDER BY pg_search_rank DESC
	  %s
	`, query, query, arrayToString(finalSaleIds, ","), query, query, limitQ)
	type Results struct {
		Id             string `db:"id"`
		QuantityBought string `db:"quantity_bought"`
		Total          string `db:"total"`
	}
	results := []Results{}
	if err = dbx.Select(&results, request); err != nil {
		return
	}
	if len(results) == 0 {
		return
	}
	total, _ = strconv.Atoi(results[0].Total)
	for _, r := range results {
		id, _ := strconv.Atoi(r.Id)
		ids = append(ids, id)
	}
	return
}

func arrayToString(a []int, delim string) string {
	return strings.Trim(strings.Replace(fmt.Sprint(a), " ", delim, -1), "[]")
	//return strings.Trim(strings.Join(strings.Split(fmt.Sprint(a), " "), delim), "[]")
	//return strings.Trim(strings.Join(strings.Fields(fmt.Sprint(a)), delim), "[]")
}

func sanitize(term string) (query string) {
	query = ""
	for _, ch := range term {
		if ch == '\'' || ch == '?' || ch == '\\' || ch == ':' || ch == ';' {
			query += " "
		} else {
			query += string(ch)
		}
	}

	return
}

func updateDb() {
	for {
		select {
		case <-quitDB:
			return
		case event := <-eventDB:
			email := event.Email
			timestamp := event.Timestamp

			if email == "" || timestamp == 0 {
				return
			}

			unixDate := time.Unix(timestamp, 0)
			occurredAt := unixDate.Format(time.RFC3339)
			url := event.Url

			switch event.Event {
			case "open":
				q := fmt.Sprintf("UPDATE email_subscriptions SET opened_at = '%s' WHERE email = '%s'", occurredAt, email)
				_, err = db.Exec(q)
				if err != nil {
					log.Fatalf("Unable to register open event: %v\n", err)
				}
			case "click":
				clicked_url := url[0:min(len(url)-1, 254)]
				q := fmt.Sprintf("UPDATE email_subscriptions SET (clicked_at, last_clicked_url) = ('%s', '%s') WHERE email = '%s'", occurredAt, clicked_url, email)
				_, err = db.Exec(q)
				if err != nil {
					log.Fatalf("Unable to register click event: %v\n", err)
				}
			}
		}
	}
}

func min(a, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}

func configureNewRelic() {
	agent := gorelic.NewAgent()
	agent.Verbose = true
	agent.NewrelicLicense = os.Getenv("NEW_RELIC_LICENSE_KEY")
	agent.NewrelicName = "Go baligam events handler"
	agent.Run()
}
