class ArticleData {
    constructor(title, author, date, ID, articleLink, visLink, plainText, highlightData) {
        this.title = title;
        this.author = author;
        this.date = date.slice(0, -18);
        this.id = ID;
        this.articleLink = articleLink;
        this.plainText = plainText;
        this.visLink = visLink;
        this.highlightData = highlightData;
        this.credibilityScore = 100;
    }
}
