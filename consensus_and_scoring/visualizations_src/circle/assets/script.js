/* keep track of position 1, 2, 3 */

/*
if (pos_1 == True):
    make CSS look and act like this
elif (pos_2 == True):
    make CSS look and act like this_2
elif (pos_3 == True):
    make CSS look and act liek this_3
*/
let pos_1 = true;
let pos_2 = false;
let pos_3 = false;

var spin_ring = document.getElementsByClassName('ring')[0]
var arc = document.getElementsByClassName('arc')[0]
var pos_num = document.getElementsByClassName('position')[0]
var tooltip = document.getElementsByClassName('tool-desc')[0]
var tooltitle = document.getElementsByClassName('tool-title')[0]

function changePos() {

//Currently pos_1 and "it got clicked" so do all these things and then move to pos_2//
    if (pos_1) {
        /* If position 1 and clicked move to pos_2 */
        spin_ring.style.transform = 'rotate(120deg)';
        arc.style.background = '#EF7559';
        /* we don't necessarily want words in side the ring thing
        pos_num.innerHTML = 'pos_2'
        */
        tooltip.innerHTML = "Hi! I'm the tooltip #2 that's supposed to describe the differences between tool tips 1 and 3! I'm orange-red!"
        tooltitle.innerHTML = "Tool's title 2"
        pos_1 = false;
        pos_2 = true;
    }
    else if (pos_2) {
        /* If position 2 and clicked move to pos_3 */
        spin_ring.style.transform = 'rotate(240deg)'
        arc.style.background = '#FFB400';
        /* we don't necessarily want words in side the ring thing
        pos_num.innerHTML = 'pos_3'
        */
        tooltip.innerHTML = "Hi! I'm the tooltip #3 that's supposed to describe the differences between tool tips 1 and 2! I'm yellow!"
        tooltitle.innerHTML = "Tool's title 3"
        pos_2 = false;
        pos_3 = true;
    }
    else if (pos_3) {
        /* If position 3 and clicked move to pos_1 */
        spin_ring.style.transform = 'rotate(360deg)'
        arc.style.background = '#10D4C6';
        /* we don't necessarily want words in side the ring thing
        pos_num.innerHTML = 'pos_1'
        */
        tooltip.innerHTML = "Hi! I'm the tooltip #1 that's supposed to describe the differences between tool tips 2 and 3! I'm cyan!"
        tooltitle.innerHTML = "Tool's title 1"
        pos_3 = false;
        pos_1 = true;
    }
}

