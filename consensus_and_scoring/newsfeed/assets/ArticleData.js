class ArticleData {
    constructor(title, author, date, ID, visLink, plainText, highlightData, triageData, article_sha256) {
        this.title = title;
        this.author = author;
        this.date = date;
        this.id = ID;
        this.sha256= article_sha256;
        this.plainText = plainText;
        this.visLink = visLink;
        this.highlightData = highlightData;
        this.triageData = triageData;
        this.credibilityScore = 100;
        this.totalText = "";
    }
}
