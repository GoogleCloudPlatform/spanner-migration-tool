package performance

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

func RandomInt(min, max int64) int64 {
	return min + rand.Int63n(max-min+1)
}

func RandomFloat(min, max int64) float64 {
	return float64(min) + rand.Float64()*float64(max-min)
}

const alphabet = "abcdefghijklmnopqrstuvwxyz"

func RandomString(n int) string {
	var sb strings.Builder
	k := len(alphabet)

	for i := 0; i < n; i++ {
		c := alphabet[rand.Intn(k)]
		sb.WriteByte(c)
	}

	return sb.String()
}

func RandomDate() string {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2010, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	delta := max - min

	sec := rand.Int63n(delta) + min
	return time.Unix(sec, 0).Format("2006-01-02")
}

func RandomBool() bool {
	i := RandomInt(0, 10)
	return i%2 == 0
}

func CurrentTimestamp() string {
	return time.Now().Format("2006-01-02 15:04:05")
}

func GetMYSQLConnectionStr(server, port, user, password, dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, server, port, dbName)
}
