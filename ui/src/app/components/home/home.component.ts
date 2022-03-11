import { Component, OnInit } from '@angular/core'
import { LoaderService } from 'src/app/services/loader/loader.service'

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.scss'],
})
export class HomeComponent implements OnInit {
  constructor() {}
  showProgress = false;

  ngOnInit(): void {
    var loader = new LoaderService()
    loader.startLoader();
    loader.isLoading.subscribe( data => this.showProgress = data)
  }
}
