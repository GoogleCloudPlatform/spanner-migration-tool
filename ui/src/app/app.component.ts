import { Component } from '@angular/core';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss']
})
export class AppComponent {
  title = 'ui';
  showProgress = false;
  ngOnInit() {
    setInterval(
      () => { this.showProgress = !this.showProgress; }
      , 5000);
  }
}

