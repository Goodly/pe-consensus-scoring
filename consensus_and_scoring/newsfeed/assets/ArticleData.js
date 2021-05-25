class ArticleData {
    constructor(title, author, date, ID, articleLink, visLink, plainText, highlightData, article_sha256) {
        this.title = title;
        this.author = author;
        this.date = date;
        this.id = ID;
        this.sha256= article_sha256;
        this.articleLink = articleLink;
        this.plainText = plainText;
        this.visLink = visLink;
        this.highlightData = highlightData;
        this.credibilityScore = 100;
    }
}
