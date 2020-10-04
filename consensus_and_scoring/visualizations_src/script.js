
function display(type_bar) {
  var ctx = document.getElementById('myChart');

  var myChart = new Chart(ctx, {
      type: type_bar,
      data: {
          labels: ['Evidence', 'Holistic', 'Language', 'Probability', 'Reasoning'],
          datasets: []
      }

  });
  return myChart
}
// datasets
// {
//     label: name,
//     data: scores, /* Reference from scores */
//     backgroundColor: [
//         /* Article 1 will be this color (pink) ??? */
//         background_color,
//         background_color,
//         background_color,
//         background_color,
//         background_color,
//     ],
//     borderColor: [
//         /* The first label's point will be this color */
//         background_color,
//     ],
//     borderWidth: 1
// }

function draw(type, article_name, scores, background_color) {
  // create dataset for the score:
  // label: article_name
  // data: [scores]
  // backgroundColor: [background_color]
  // borderColor: [background_color]
  // borderWidth: 1
  // chart.get_data.get_datasets.append(new_dataset)
  new_dataset = {
    label: article_name,
    data: scores,
    backgroundColor: [
      background_color,
      background_color,
      background_color,
      background_color,
      background_color,
    ],
    borderColor: [
      background_color
    ],
    borderWidth: 1
  }
  chart = display(type)
  chart["data"]["datasets"].push(new_dataset)
}
