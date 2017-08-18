
package main

import "github.com/PuerkitoBio/goquery"
import "log"

func main() {
	doc, err := goquery.NewDocument("http://studygolang.com/topics")
	if err != nil {
		log.Fatal(err)
	}

	doc.Find(".topics .topic").Each(func(i int, contentSelection *goquery.Selection) {
		title := contentSelection.Find(".title a").Text()
		log.Println("第", i+1, "个帖子的标题：", title)
	})
}
