SCRAPE_CHARACTER_PROFILES = '''
() => {
characters = [];
var character_profiles = document.getElementsByClassName('profile-card-link')
for(var i = 0; i < character_profiles.length; i++) {
try{
    characters.push([character_profiles[i].getElementsByClassName('info')[0].getElementsByClassName('info-name')[0].innerText,
    character_profiles[i].href,
    character_profiles[i].getElementsByClassName('vote-count')[0].innerText]);    
}
catch (e){
    continue;
}    
    }
return characters
}
'''

SCRAPE_MBTI_SCORES_SCRIPT = '''
() => {
mbti_scores = [];
var mbti_letters = document.getElementsByClassName('vote-detail-letter')
for(var i = 0; i < mbti_letters.length; i++) {
    mbti_scores.push(mbti_letters[i].innerText);
    }
mbti_scores.push(document.getElementsByClassName('personality-vote-count')[0].innerText)
return mbti_scores
}
'''