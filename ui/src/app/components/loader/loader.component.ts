import { Component, OnInit } from '@angular/core'
import { LoaderService } from 'src/app/services/loader/loader.service'

@Component({
  selector: 'app-loader',
  templateUrl: './loader.component.html',
  styleUrls: ['./loader.component.scss'],
  providers: [LoaderService],
})
export class LoaderComponent implements OnInit {
  showProgress: boolean = true
  constructor(private loaderService: LoaderService) {}

  ngOnInit(): void {
    this.loaderService.isLoading.subscribe((data) => (this.showProgress = data))
  }
}
