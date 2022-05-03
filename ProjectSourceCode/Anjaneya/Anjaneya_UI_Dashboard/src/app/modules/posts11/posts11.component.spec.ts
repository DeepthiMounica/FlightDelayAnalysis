import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Posts11Component } from './posts11.component';

describe('Posts11Component', () => {
  let component: Posts11Component;
  let fixture: ComponentFixture<Posts11Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Posts11Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Posts11Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
