import { NgModule } from '@angular/core';
import { Routes, RouterModule } from '@angular/router';
import { DefaultComponent } from './layouts/default/default.component';
import { DashboardComponent } from './modules/dashboard/dashboard.component';
import { PostsComponent } from './modules/posts/posts.component';
import { Posts1Component } from './modules/posts1/posts1.component';
import { Posts10Component } from './modules/posts10/posts10.component';
import { Posts2Component } from './modules/posts2/posts2.component';
import { Posts3Component } from './modules/posts3/posts3.component';
import { Posts4Component } from './modules/posts4/posts4.component';
import { Posts5Component } from './modules/posts5/posts5.component';
import { Posts6Component } from './modules/posts6/posts6.component';
import { Posts61Component } from './modules/posts61/posts61.component';
import { Posts7Component } from './modules/posts7/posts7.component';
import { Posts8Component } from './modules/posts8/posts8.component';
import { Posts9Component } from './modules/posts9/posts9.component';


const routes: Routes = [{
  path: '',
  component: DefaultComponent,
  children: [{
    path: '',
    component: DashboardComponent
  }, {
    path: 'posts',
    component: PostsComponent
  },
  {
    path: 'posts1',
    component: Posts1Component
  },
  {
    path: 'posts2',
    component: Posts2Component
  },
  {
    path: 'posts3',
    component: Posts3Component
  },
  {
    path: 'posts4',
    component: Posts4Component
  },
  {
    path: 'posts5',
    component: Posts5Component
  },
  {
    path: 'posts6',
    component: Posts6Component
  },
  {
    path: 'posts7',
    component: Posts7Component
  },
  {
    path: 'posts8',
    component: Posts8Component
  },
  {
    path: 'posts9',
    component: Posts9Component
  }
  ,
  {
    path: 'posts10',
    component: Posts10Component
  },
  {
    path: 'posts61',
    component: Posts61Component
  }
]
}];

@NgModule({
  imports: [RouterModule.forRoot(routes)],
  exports: [RouterModule]
})
export class AppRoutingModule { }
