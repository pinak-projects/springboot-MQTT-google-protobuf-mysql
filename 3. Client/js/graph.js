		
		var smoothie = new SmoothieChart({interpolation:'linear', maxValue:1.5,minValue:-1.5 ,timestampFormatter:SmoothieChart.timeFormatter});
		var pp = document.getElementById("graph")
		
		
		var smoothie1 = new SmoothieChart({interpolation:'linear', maxValue:1.5,minValue:-1.5 ,timestampFormatter:SmoothieChart.timeFormatter});
		var pp1 = document.getElementById("graph1")

		var smoothie2 = new SmoothieChart({interpolation:'linear', maxValue:1.5,minValue:-1.5 ,timestampFormatter:SmoothieChart.timeFormatter});
		var pp2 = document.getElementById("graph2")

		var smoothie3 = new SmoothieChart({interpolation:'linear', maxValue:1.5,minValue:-1.5 ,timestampFormatter:SmoothieChart.timeFormatter ,labels:{precision:6} ,minValueScale:6, maxValueScale:7});
		var pp3 = document.getElementById("graph3")

		var smoothie4 = new SmoothieChart({interpolation:'linear', maxValue:1.5,minValue:-1.5 ,timestampFormatter:SmoothieChart.timeFormatter});
		var pp4 = document.getElementById("graph4")
		
		smoothie.streamTo( pp );
		smoothie1.streamTo( pp1 , 1000);
		smoothie2.streamTo( pp2 , 1000);
		smoothie3.streamTo( pp3 , 1000);
		smoothie4.streamTo( pp4 , 1000);
		
		var line = new TimeSeries();
		var line1 = new TimeSeries();
		var line2 = new TimeSeries();
		var line3 = new TimeSeries();
		var line4 = new TimeSeries();
		
		var xhr = new XMLHttpRequest();
		//xhr.open('GET', '{{ url_for('stream') }}');
		xhr.open('GET', 'http://127.0.0.1:5000/get_time');
		//xhr.open('GET',  'get_time' , true);
		xhr.send();
		var position = 0;

		
		// Data
		//var line = new TimeSeries();
		var number = 0;
		function handleNewData() {
        
        var messages = xhr.responseText.split('??');
		
        //messages.slice(position, -1).forEach(function(value) {
        messages.slice(0, messages.length).forEach(function(value) {
  
		//var obj = value;
		console.log(value);
		var obj = JSON.parse(value);
		//console.log(obj);
        
		
		var sin=obj["sin"];
		//console.log(sin);
        var strl=sin.length;		
        var num = parseFloat(sin.slice(0,strl-1));
        line.append(new Date().getTime(),  num);
		
		var square=obj["square"];
		//console.log(sin);
        var strl=sin.length;		
        var num1 = parseFloat(square.slice(0,strl-1));
        line1.append(new Date().getTime(),  num1);
		
		var square_d=obj["square_d"];
		//console.log(square_d);
        var strl=square_d.length;		
        var num2 = parseFloat(square_d);
		//console.log(num2);
        line2.append(new Date().getTime(),  num2);
		
		var triangle=obj["triangle"];
		//console.log(triangle);
        var strl=triangle.length;		
        var num3 = parseFloat(triangle.slice(0,strl));
        line3.append(new Date().getTime(),  num3);
		
		var smooth=obj["smooth"];
		//console.log(sin);
        var strl=smooth.length;		
        var num4 = parseFloat(smooth.slice(0,strl-1));
        line4.append(new Date().getTime(),  num4);
        
		
        //console.log("number"+obj["number"]);
		//if(number+1 != obj["number"]){
		//console.log("miss");		}
		
		//number = obj["number"];
		});
        position = messages.length - 1;
    }
	smoothie.addTimeSeries(line , {lineWidth:2,strokeStyle:'#00ff00'});
	smoothie1.addTimeSeries(line1 , {lineWidth:2,strokeStyle:'#00ff00'});
	smoothie2.addTimeSeries(line2 , {lineWidth:2,strokeStyle:'#00ff00'});
	smoothie3.addTimeSeries(line3 , {lineWidth:2,strokeStyle:'#00ff00'});
	smoothie4.addTimeSeries(line4 , {lineWidth:2,strokeStyle:'#00ff00'});
	console.log("something");
	
    /*var timer;
    timer = setInterval(function() {
        // check the response for new data
        handleNewData();
        // stop checking once the response has ended
        if (xhr.readyState == XMLHttpRequest.DONE) {
            clearInterval(timer);
            
        }
    }
	, 50);
*/	handleNewData();
console.log("something");

	if(xhr.responseText){
	console.log("message");}
	smoothie.addTimeSeries(line);
		