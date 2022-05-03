import { BrowserModule } from '@angular/platform-browser';
import { NgModule } from '@angular/core';

import { AppRoutingModule } from './app-routing.module';
import { AppComponent } from './app.component';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { DefaultModule } from './layouts/default/default.module';
import { Posts1Component } from './modules/posts1/posts1.component';
import { Posts2Component } from './modules/posts2/posts2.component';
import { Posts3Component } from './modules/posts3/posts3.component';
import { Posts4Component } from './modules/posts4/posts4.component';
import { Posts5Component } from './modules/posts5/posts5.component';
import { Posts6Component } from './modules/posts6/posts6.component';
import { Posts7Component } from './modules/posts7/posts7.component';
import { Posts8Component } from './modules/posts8/posts8.component';
import { Posts9Component } from './modules/posts9/posts9.component';
import { Posts10Component } from './modules/posts10/posts10.component';
import { Posts11Component } from './modules/posts11/posts11.component';
import { Posts61Component } from './modules/posts61/posts61.component';

@NgModule({
  declarations: [
    AppComponent,
    Posts1Component,
    Posts2Component,
    Posts3Component,
    Posts4Component,
    Posts5Component,
    Posts6Component,
    Posts7Component,
    Posts8Component,
    Posts9Component,
    Posts10Component,
    Posts11Component,
    Posts61Component,
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    BrowserAnimationsModule,
    DefaultModule
  ],
  providers: [],
  bootstrap: [AppComponent]
})
export class AppModule { }
